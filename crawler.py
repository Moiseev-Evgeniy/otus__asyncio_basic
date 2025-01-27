import asyncio
import logging
from aiohttp import ClientSession, ServerDisconnectedError, ClientConnectionError
from bs4 import BeautifulSoup
import aiofiles.os


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s")
logger.setLevel(logging.INFO)

URL = "https://news.ycombinator.com/"

async def save_to_file(url: str, parent_url: str, data: str):
    dir_name = parent_url.split("//")[1].replace('/', '-')
    filename = url.split("//")[1].replace('/', '-')
    if len(dir_name) > 255:
        dir_name = dir_name[:255]
    if len(filename) > 255:
        filename = filename[:255]

    if not await aiofiles.os.path.exists(f"data/{dir_name}"):
        await aiofiles.os.mkdir(f"data/{dir_name}")

    async with aiofiles.open(f"data/{dir_name}/{filename}", "w", encoding="utf-8") as f:
        await f.write(data)


async def get_response(url: str):
    retry = 2
    retry_delay = 0.5

    while retry >= 0:
        retry -= 1

        logger.info(f"Sending request: GET {url}")
        try:
            async with ClientSession() as session:
                async with session.get(url, timeout=5) as response:
                        return await response.text(encoding="utf-8")

        except asyncio.TimeoutError:
            logger.error(f"Timeout error. Retry in {retry_delay} s...")
        except ServerDisconnectedError:
            logger.error(f"Server disconnected. Retry in {retry_delay} s...")
        except ClientConnectionError:
            logger.error(f"Connection error. Retry in {retry_delay} s...")
        except UnicodeDecodeError:
            logger.error(f"Can't decode data. Break.")
            break

async def parse(url: str, first_link: str | None = None, is_comments: bool = False, parent_url: str | None = None):

    response = await get_response(url)
    soup = BeautifulSoup(response, "html.parser")

    last_link = None
    current_link = parent_url

    for link in soup.find_all("a"):
        href = link.get("href")

        if "ycombinator" in href or "github.com/HackerNews/API" in href:
            continue

        if href.startswith("http"):
            if not is_comments:
                if first_link is None:
                    first_link = href
                    last_link = 1
                elif first_link == href or last_link == href:
                    break
                elif first_link != href and not last_link:
                    last_link = first_link
                    first_link = href

                current_link = href

            if response := await get_response(href):
                logger.info(f"Save to file: {href}")
                asyncio.create_task(save_to_file(href, current_link, response))
            continue

        if not is_comments and "\xa0comment" in link.decode_contents():
            asyncio.create_task(parse(URL + href, is_comments=True, parent_url=current_link))

    return first_link

async def main(url: str):
    first_link = None
    while True:
        first_link = await parse(url, first_link)
        logger.info("Waiting for new links...")
        await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(main(URL))
