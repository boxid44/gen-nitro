import asyncio
import aiohttp
import random
import string
import time
import sys

BASE_URL = "https://discord.com/billing/promotions/"
# ----------------------------------------------------------
DISCORD_WEBHOOK_URL = "add your own webhook"
# ----------------------------------------------------------
MAX_CONCURRENT_REQUESTS = 80  
QUEUE_SIZE = 300
STATS_INTERVAL = 5.0

class RateLimiter:
    def __init__(self):
        self.current_delay = 0.0
        self.is_blocked = False
        self.backoff_tier = 0
        self.lock = asyncio.Lock()

    async def report_success(self):
        async with self.lock:
            if self.backoff_tier > 0:
                self.backoff_tier = max(0, self.backoff_tier - 1)
                if self.backoff_tier == 0:
                    self.is_blocked = False
                    self.current_delay = 0.0

    async def report_rate_limit(self, status_code: int):
        async with self.lock:
            self.backoff_tier += 1
            self.is_blocked = True
            self.current_delay = float(2 ** self.backoff_tier)
            print(f"\n[BLOKADA API] Kod {status_code}. Anty-spam lock na {self.current_delay}s...")
            await asyncio.sleep(self.current_delay)
            self.is_blocked = False

    async def wait_if_needed(self):
        if self.is_blocked:
            await asyncio.sleep(0.5)

class Statistics:
    def __init__(self):
        self.total_checked = 0
        self.total_valid = 0
        self.start_time = time.time()
        self.last_report_time = time.time()
        self.last_checked_count = 0

    def increment_checked(self):
        self.total_checked += 1

    def increment_valid(self):
        self.total_valid += 1

    async def run_reporter(self):
        while True:
            await asyncio.sleep(STATS_INTERVAL)
            now = time.time()
            elapsed_chunk = now - self.last_report_time
            checked_chunk = self.total_checked - self.last_checked_count
            avg_speed_chunk = checked_chunk / elapsed_chunk if elapsed_chunk > 0 else 0
            
            sys.stdout.write(
                f"\r checked: {self.total_checked} | "
                f"hit: {self.total_valid} | "
                f"speed: {avg_speed_chunk:.2f} req/s"
            )
            sys.stdout.flush()
            self.last_report_time = now
            self.last_checked_count = self.total_checked

class AsyncFileWriter:
    def __init__(self, filename: str = "poprawne_kody.txt"):
        self.filename = filename
        self.loop = asyncio.get_event_loop()

    def _sync_write(self, text: str):
        with open(self.filename, "a", encoding="utf-8") as f:
            f.write(text)
            f.flush()

    async def append_line(self, text: str):
        await self.loop.run_in_executor(None, self._sync_write, f"{text}\n")

async def send_to_discord(session: aiohttp.ClientSession, message: str):
    if not DISCORD_WEBHOOK_URL or DISCORD_WEBHOOK_URL == "add your own webhook":
        return
    try:
        await session.post(DISCORD_WEBHOOK_URL, json={"content": message})
    except Exception:
        pass

async def producer(queue: asyncio.Queue):
    chars = string.ascii_uppercase + string.ascii_lowercase + string.digits
    while True:
        if queue.full():
            await asyncio.sleep(0.1)
            continue
        for _ in range(50):
            first_char = random.choice(string.ascii_uppercase)
            rest_of_code = ''.join(random.choices(chars, k=14))
            await queue.put(f"{BASE_URL}{first_char}{rest_of_code}")
        await asyncio.sleep(0.01)

async def worker(queue: asyncio.Queue, session: aiohttp.ClientSession, 
                 semaphore: asyncio.Semaphore, limiter: RateLimiter, stats: Statistics, writer: AsyncFileWriter):
    while True:
        url = await queue.get()
        await limiter.wait_if_needed()

        async with semaphore:
            try:
                timeout = aiohttp.ClientTimeout(connect=3.0, sock_read=5.0)
                async with session.get(url, timeout=timeout, allow_redirects=False) as response:
                    stats.increment_checked()

                    if response.status == 429 or response.status >= 500:
                        await limiter.report_rate_limit(response.status)
                        await queue.put(url)
                        queue.task_done()
                        continue

                    await limiter.report_success()

                    if response.status in [301, 302]:
                        queue.task_done()
                        continue

                    if response.status == 200:
                        page_text = await response.text()
                        if any(p in page_text.lower() for p in ["invalid", "błąd", "expired", "not found", "error"]):
                            queue.task_done()
                            continue
                        
                        stats.increment_valid()
                        print(f"\n\n🎉 [PC TRAFIŁ KOD!] {url}\n")
                        await writer.append_line(f"{url} - poprawny")
                        await send_to_discord(session, f"code find correct**\nLink: {url}")
                        
            except Exception:
                pass
            finally:
                queue.task_done()

async def main():
    queue = asyncio.Queue(maxsize=QUEUE_SIZE)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    limiter = RateLimiter()
    stats = Statistics()
    writer = AsyncFileWriter()

    connector = aiohttp.TCPConnector(limit=None, keepalive_timeout=30.0, enable_cleanup_closed=True)

    async with aiohttp.ClientSession(connector=connector) as session:
        producer_task = asyncio.create_task(producer(queue))
        reporter_task = asyncio.create_task(stats.run_reporter())
        workers = [asyncio.create_task(worker(queue, session, semaphore, limiter, stats, writer)) for _ in range(MAX_CONCURRENT_REQUESTS)]

        try:
            await asyncio.gather(producer_task, reporter_task, *workers)
        except KeyboardInterrupt:
            pass
        finally:
            producer_task.cancel()
            reporter_task.cancel()
            for w in workers: w.cancel()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())