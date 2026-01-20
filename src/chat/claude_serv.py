from urllib.parse import parse_qs, urlparse
import asyncio
import websockets
import sys
import msgpack
from collections import defaultdict
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import os

from chat.utils import mk_pack
from chat.protocoltypes import Command, PackIDX, Snowflake


MAX_QUEUE_PER_CLIENT = 1000
MAX_INBOUND_PER_CLIENT = 200
MAX_CLIENTS = 10000


class ClientSession:
    __slots__ = ('ws', 'server', 'inbound', 'queue', 'closed', 'channels', 
                 'writer_task', 'id')
    
    def __init__(self, ws, server, client_id):
        self.ws = ws
        self.server: ChatServer = server
        self.id = client_id
        self.inbound = asyncio.Queue(maxsize=MAX_INBOUND_PER_CLIENT)
        self.queue = asyncio.Queue(maxsize=MAX_QUEUE_PER_CLIENT)
        self.closed = False
        self.channels: set[int] = set()
        self.writer_task = asyncio.create_task(self.writer())

    async def writer(self):
        """Write messages with batching"""
        try:
            while True:
                msg = await self.queue.get()
                
                try:
                    await self.ws.send(msg)
                    self.server.msg_out += 1
                except websockets.ConnectionClosed:
                    break
                except Exception as e:
                    print(f"Send error: {e}")
                    break
                
                # Batch sends
                sent_count = 1
                while sent_count < 20 and not self.queue.empty():
                    try:
                        next_msg = self.queue.get_nowait()
                        await self.ws.send(next_msg)
                        self.server.msg_out += 1
                        sent_count += 1
                    except asyncio.QueueEmpty:
                        break
                    except websockets.ConnectionClosed:
                        return
                    except Exception:
                        break
                
                if sent_count > 5:
                    await asyncio.sleep(0)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Writer error: {e}")
        finally:
            self.closed = True

    def send_nowait(self, msg):
        """Non-blocking send"""
        try:
            self.queue.put_nowait(msg)
            return True
        except asyncio.QueueFull:
            return False

    async def cleanup(self):
        """Clean shutdown"""
        self.writer_task.cancel()
        await asyncio.gather(self.writer_task, return_exceptions=True)


# Message processing worker (runs in separate process)
def process_message_worker(input_queue: mp.Queue, output_queue: mp.Queue):
    """Worker process for message processing - runs on dedicated core"""
    print(f"Message processor started on PID {os.getpid()}")
    
    while True:
        try:
            # Get batch of raw messages
            batch = []
            item = input_queue.get()
            if item is None:  # Poison pill
                break
            
            batch.append(item)
            
            # Drain queue
            while len(batch) < 50:
                try:
                    item = input_queue.get_nowait()
                    if item is None:
                        break
                    batch.append(item)
                except:
                    break
            
            # Process batch
            results = []
            for client_id, raw_msg in batch:
                try:
                    msg = msgpack.unpackb(raw_msg, strict_map_key=False)
                    results.append((client_id, msg))
                except Exception as e:
                    print(f"Failed to unpack: {e}")
            
            # Send results back
            if results:
                output_queue.put(results)
                
        except Exception as e:
            print(f"Processor error: {e}")


class ChatServer:
    def __init__(self, use_multicore=True):
        self.clients: dict[int, ClientSession] = {}
        self.channels: dict[int, dict[int, str]] = defaultdict(dict)
        self.sf = Snowflake(22)
        self.msg_in: int = 0
        self.msg_out: int = 0
        self.last_msg_in = 0
        self.last_msg_out = 0
        
        # Multi-core processing setup
        self.use_multicore = use_multicore
        if use_multicore:
            # Queues for inter-process communication
            self.processing_input = mp.Queue(maxsize=10000)
            self.processing_output = mp.Queue(maxsize=10000)
            
            # Start worker processes
            self.num_processors = min(4, mp.cpu_count() - 2)  # Leave 2 cores for I/O
            self.processors = []
            for i in range(self.num_processors):
                p = mp.Process(
                    target=process_message_worker,
                    args=(self.processing_input, self.processing_output),
                    daemon=True
                )
                p.start()
                self.processors.append(p)
            
            print(f"Started {self.num_processors} message processor workers")

    async def start_message_consumer(self):
        """Consume processed messages from worker processes"""
        if not self.use_multicore:
            return
            
        async def consume():
            while True:
                # Check for processed messages (non-blocking)
                try:
                    # Use run_in_executor to avoid blocking
                    loop = asyncio.get_running_loop()
                    results = await loop.run_in_executor(
                        None, 
                        self.processing_output.get,
                        True,  # block
                        0.1    # timeout
                    )
                    
                    # Handle batch of processed messages
                    for client_id, msg in results:
                        session = self.clients.get(client_id)
                        if session:
                            await self.handle_message(session, msg)
                            
                except Exception:
                    # Timeout or empty queue
                    await asyncio.sleep(0.001)
        
        asyncio.create_task(consume())

    async def register(self, ws, name):
        if len(self.clients) >= MAX_CLIENTS:
            try:
                await ws.close(code=1013, reason="Server at capacity")
            except Exception:
                pass
            return None

        id = self.sf.next_id()
        session = ClientSession(ws, self, id)
        self.clients[id] = session
        
        try:
            await session.queue.put(str(id))
        except asyncio.QueueFull:
            await self.unregister(id)
            return None
            
        return id

    async def unregister(self, id):
        session = self.clients.pop(id, None)
        if not session:
            return
        
        for chan_id in list(session.channels):
            self.channels[chan_id].pop(id, None)
        
        await session.cleanup()

    async def leave_channel(self, id, chan_id, name: str):
        session = self.clients.get(id)
        if not session or chan_id not in session.channels:
            return

        session.channels.discard(chan_id)
        self.channels[chan_id].pop(id, None)
        
        msg = (Command.LEAVE_CHANNEL_RESP, id, name, "LEFT", chan_id, id, None)
        await self.write_to_channel(msg, exclude_id=id)

    async def register_on_channel(self, id: int, chan_id: int, name: str):
        session = self.clients.get(id)
        if not session:
            return

        self.channels[chan_id][id] = name
        session.channels.add(chan_id)
        
        msg = mk_pack(Command.JOIN_CHANNEL_RESP, id, name, "", chan_id, id)
        wlc_msg = (Command.WELCOME_TO_CHANNEL, id, name, "", chan_id, id)
        
        if not session.send_nowait(msg):
            await self.unregister(id)
            return

        await self.write_to_channel(wlc_msg)

    async def send_chan_list(self, msg):
        id = msg[PackIDX.ID]
        session = self.clients.get(id)
        if not session:
            return

        chan_id = msg[PackIDX.CHANNEL]
        content = self.channels[chan_id].copy()
        new_msg = mk_pack(
            Command.CHAN_LIST_RESP, id, msg[PackIDX.NAME], content, chan_id, to=id
        )

        if not session.send_nowait(new_msg):
            await self.unregister(id)

    async def write_to_channel(self, msg: tuple, exclude_id=None):
        """Optimized channel broadcast"""
        chan = int(msg[PackIDX.CHANNEL])
        message = mk_pack(*msg)
        
        members = self.channels[chan]
        if not members:
            return
        
        dead_clients = []
        for i, client_id in enumerate(members.keys()):
            if client_id == exclude_id:
                continue
            
            session = self.clients.get(client_id)
            if not session:
                dead_clients.append(client_id)
                continue

            if not session.send_nowait(message):
                dead_clients.append(client_id)
            
            if i % 100 == 0 and i > 0:
                await asyncio.sleep(0)
        
        if dead_clients:
            asyncio.create_task(self._cleanup_clients(dead_clients))

    async def _cleanup_clients(self, client_ids):
        for client_id in client_ids:
            await self.unregister(client_id)

    async def broadcast(self, message: bytes):
        client_ids = list(self.clients.keys())
        dead_clients = []

        for i, client_id in enumerate(client_ids):
            session = self.clients.get(client_id)
            if not session:
                continue
                
            if not session.send_nowait(message):
                dead_clients.append(client_id)
            
            if i % 100 == 0 and i > 0:
                await asyncio.sleep(0)

        if dead_clients:
            asyncio.create_task(self._cleanup_clients(dead_clients))

    async def stats_printer(self):
        while True:
            await asyncio.sleep(5)
            delta_in = self.msg_in - self.last_msg_in
            delta_out = self.msg_out - self.last_msg_out

            total_clients = len(self.clients)
            total_queued = sum(
                session.queue.qsize() for session in self.clients.values()
            )

            channel_info = "\n".join(
                f"[CHANNEL {x}] = {len(y)}" for x, y in self.channels.items() if y
            )
            if channel_info:
                print(channel_info)

            proc_queue_size = self.processing_input.qsize() if self.use_multicore else 0
            
            print(
                f"-----\nclients={total_clients}"
                f"\nqueued_out={total_queued}"
                f"\nprocessing_queue={proc_queue_size}"
                f"\nin={delta_in}/s ({delta_in/5:.0f} msg/s)"
                f"\nout={delta_out}/s ({delta_out/5:.0f} msg/s)"
            )
            self.last_msg_in = self.msg_in
            self.last_msg_out = self.msg_out

    async def handle_connection(self, ws):
        query = urlparse(ws.request.path).query
        params = parse_qs(query)
        
        if "username" not in params:
            await ws.close(code=1002, reason="Missing username")
            return
            
        name = params["username"][0]

        id = await self.register(ws, name)
        if id is None:
            return

        session = self.clients.get(id)
        if not session:
            return

        try:
            async for raw in ws:
                if self.use_multicore:
                    # Send to worker process for unpacking
                    try:
                        self.processing_input.put_nowait((id, raw))
                    except:
                        # Queue full, drop message
                        pass
                else:
                    # Single-core mode: process inline
                    try:
                        msg = msgpack.unpackb(raw, strict_map_key=False)
                        await self.handle_message(session, msg)
                    except Exception as e:
                        print(f"Processing error: {e}")
                    
        except websockets.ConnectionClosedError:
            pass
        except websockets.ConnectionClosedOK:
            pass
        except Exception as e:
            print(f"Connection error for client {id}: {e}")
        finally:
            await self.unregister(id)

    async def handle_message(self, session: ClientSession, msg: tuple):
        self.msg_in += 1
        id = msg[PackIDX.ID]
        name = msg[PackIDX.NAME]

        match msg[PackIDX.COMMAND]:
            case Command.JOIN_CHANNEL:
                chan_id = msg[PackIDX.CHANNEL]
                await self.register_on_channel(id, chan_id, name)
            case Command.LEAVE_CHANNEL:
                chan_id = msg[PackIDX.CHANNEL]
                await self.leave_channel(id, chan_id, name)
            case Command.WELCOME_TO_CHANNEL:
                await self.write_to_channel(msg)
            case Command.WRITE_TO_CHANNEL:
                await self.write_to_channel(msg)
            case Command.LEAVE_CHANNEL_RESP:
                await self.write_to_channel(msg)
            case Command.CHAN_LIST:
                await self.send_chan_list(msg)
            case _:
                print(f"Unhandled command: {Command(msg[PackIDX.COMMAND]).name}")

    def shutdown(self):
        """Cleanup worker processes"""
        if self.use_multicore:
            # Send poison pills
            for _ in self.processors:
                self.processing_input.put(None)
            
            # Wait for workers to finish
            for p in self.processors:
                p.join(timeout=1)
                if p.is_alive():
                    p.terminate()


async def run():
    # Enable multicore mode with --multicore flag
    use_multicore = '--multicore' in sys.argv
    
    server = ChatServer(use_multicore=use_multicore)
    port = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 8765

    if use_multicore:
        await server.start_message_consumer()
        print("Running in MULTI-CORE mode")
    else:
        print("Running in SINGLE-CORE mode")

    try:
        async with websockets.serve(
            server.handle_connection,
            "localhost",
            int(port),
            ping_interval=None,
            ping_timeout=None,
            close_timeout=2,
            max_size=2**20,
            compression=None,
        ):
            asyncio.create_task(server.stats_printer())
            print(f"Server started on ws://localhost:{port}")
            await asyncio.Future()
    finally:
        server.shutdown()


def main():
    # Required for multiprocessing on some platforms
    mp.set_start_method('spawn', force=True)
    asyncio.run(run())


if __name__ == "__main__":
    main()
