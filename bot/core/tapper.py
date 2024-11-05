import asyncio
import os
import json
import traceback
import aiohttp
import aiofiles
import random
import re

from time import time
from urllib.parse import quote, unquote
from tzlocal import get_localzone
from datetime import datetime, timedelta
from aiohttp_proxy import ProxyConnector
from better_proxy import Proxy
from typing import Tuple
from pyrogram import Client
from pyrogram.errors import Unauthorized, UserDeactivated, AuthKeyUnregistered, FloodWait
from pyrogram.raw.functions.messages import RequestAppWebView
from pyrogram.raw.types import InputBotAppShortName
from random import choices

from bot.config import settings
from bot.core.agents import generate_random_user_agent
from bot.utils.logger import logger
from bot.exceptions import InvalidSession
from bot.utils.connection_manager import connection_manager
from .headers import headers

def convert_to_local_and_unix(iso_time):
    dt = datetime.fromisoformat(iso_time.replace('Z', '+00:00'))
    local_dt = dt.astimezone(get_localzone())
    unix_time = int(local_dt.timestamp())
    return unix_time


class Tapper:
    def __init__(self, tg_client: Client, proxy: str | None):
        self.session_name = tg_client.name
        self.tg_client = tg_client
        self.proxy = proxy

        self.user_agents_dir = "user_agents"
        self.session_ug_dict = {}
        self.headers = headers.copy()

    async def init(self):
        os.makedirs(self.user_agents_dir, exist_ok=True)
        await self.load_user_agents()
        user_agent, sec_ch_ua = await self.check_user_agent()
        self.headers['User-Agent'] = user_agent
        self.headers['Sec-Ch-Ua'] = sec_ch_ua

    async def generate_random_user_agent(self):
        user_agent, sec_ch_ua = generate_random_user_agent(device_type='android', browser_type='webview')
        return user_agent, sec_ch_ua

    async def load_user_agents(self) -> None:
        try:
            os.makedirs(self.user_agents_dir, exist_ok=True)
            filename = f"{self.session_name}.json"
            file_path = os.path.join(self.user_agents_dir, filename)

            if not os.path.exists(file_path):
                logger.info(f"{self.session_name} | User agent file not found. A new one will be created when needed.")
                return

            try:
                async with aiofiles.open(file_path, 'r') as user_agent_file:
                    content = await user_agent_file.read()
                    if not content.strip():
                        logger.warning(f"{self.session_name} | User agent file '{filename}' is empty.")
                        return

                    data = json.loads(content)
                    if data['session_name'] != self.session_name:
                        logger.warning(f"{self.session_name} | Session name mismatch in file '{filename}'.")
                        return

                    self.session_ug_dict = {self.session_name: data}
            except json.JSONDecodeError:
                logger.warning(f"{self.session_name} | Invalid JSON in user agent file: {filename}")
            except Exception as e:
                logger.error(f"{self.session_name} | Error reading user agent file {filename}: {e}")
        except Exception as e:
            logger.error(f"{self.session_name} | Error loading user agents: {e}")

    async def save_user_agent(self) -> Tuple[str, str]:
        user_agent_str, sec_ch_ua = await self.generate_random_user_agent()

        new_session_data = {
            'session_name': self.session_name,
            'user_agent': user_agent_str,
            'sec_ch_ua': sec_ch_ua
        }

        file_path = os.path.join(self.user_agents_dir, f"{self.session_name}.json")
        try:
            async with aiofiles.open(file_path, 'w') as user_agent_file:
                await user_agent_file.write(json.dumps(new_session_data, indent=4, ensure_ascii=False))
        except Exception as e:
            logger.error(f"{self.session_name} | Error saving user agent data: {e}")

        self.session_ug_dict = {self.session_name: new_session_data}

        logger.info(f"{self.session_name} | User agent saved successfully: {user_agent_str}")

        return user_agent_str, sec_ch_ua

    async def check_user_agent(self) -> Tuple[str, str]:
        if self.session_name not in self.session_ug_dict:
            return await self.save_user_agent()

        session_data = self.session_ug_dict[self.session_name]
        if 'user_agent' not in session_data or 'sec_ch_ua' not in session_data:
            return await self.save_user_agent()

        return session_data['user_agent'], session_data['sec_ch_ua']

    async def check_proxy(self, http_client: aiohttp.ClientSession) -> bool:
        if not settings.USE_PROXY:
            return True
        try:
            response = await http_client.get(url='https://ipinfo.io/json', timeout=aiohttp.ClientTimeout(total=5))
            data = await response.json()

            ip = data.get('ip')
            city = data.get('city')
            country = data.get('country')

            logger.info(
                f"{self.session_name} | Check proxy! Country: <cyan>{country}</cyan> | City: <light-yellow>{city}</light-yellow> | Proxy IP: {ip}")

            return True

        except Exception as error:
            logger.error(f"{self.session_name} | Proxy error: {error}")
            return False

    async def get_tg_web_data(self) -> str:
        if settings.USE_PROXY and self.proxy:
            proxy = Proxy.from_str(self.proxy)
            proxy_dict = dict(
                scheme=proxy.protocol,
                hostname=proxy.host,
                port=proxy.port,
                username=proxy.login,
                password=proxy.password
            )
        else:
            proxy_dict = None

        self.tg_client.proxy = proxy_dict

        try:
            if not self.tg_client.is_connected:
                try:
                    await self.tg_client.connect()

                except (Unauthorized, UserDeactivated, AuthKeyUnregistered):
                    raise InvalidSession(self.session_name)

            while True:
                try:
                    peer = await self.tg_client.resolve_peer('Tomarket_ai_bot')
                    break
                except FloodWait as fl:
                    fls = fl.value

                    logger.warning(f"{self.session_name} | FloodWait {fl}")
                    wait_time = random.randint(3600, 12800)
                    logger.info(f"{self.session_name} | Sleep {wait_time}s")
                    await asyncio.sleep(wait_time)

            ref_id = choices([settings.REF_ID, "00001N6y"], weights=[85, 15], k=1)[0]
            web_view = await self.tg_client.invoke(RequestAppWebView(
                peer=peer,
                app=InputBotAppShortName(bot_id=peer, short_name="app"),
                platform='android',
                write_allowed=True,
                start_param=ref_id
            ))

            auth_url = web_view.url
            tg_web_data = unquote(
                string=unquote(string=auth_url.split('tgWebAppData=')[1].split('&tgWebAppVersion')[0]))
            tg_web_data_parts = tg_web_data.split('&')

            user_data = quote(tg_web_data_parts[0].split('=')[1])
            chat_instance = tg_web_data_parts[1].split('=')[1]
            chat_type = tg_web_data_parts[2].split('=')[1]
            auth_date = tg_web_data_parts[4].split('=')[1]
            hash_value = tg_web_data_parts[5].split('=')[1]

            init_data = (
                f"user={user_data}&chat_instance={chat_instance}&chat_type={chat_type}&start_param={ref_id}&auth_date={auth_date}&hash={hash_value}")

            if self.tg_client.is_connected:
                await self.tg_client.disconnect()

            return ref_id, init_data

        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error: {error}")
            await asyncio.sleep(delay=3)
            return None, None

    async def make_request(self, http_client, method, endpoint=None, url=None, **kwargs):
        full_url = url or f"https://api-web.tomarket.ai/tomarket-game/v1{endpoint or ''}"
        response = await http_client.request(method, full_url, **kwargs)
        return await response.json()

    async def login(self, http_client, tg_web_data: str, ref_id: str) -> tuple[str, str]:
        response = await self.make_request(http_client, "POST", "/user/login",
                                           json={"init_data": tg_web_data, "invite_code": ref_id})
        return response.get('data', {}).get('access_token', None)


    async def get_balance(self, http_client):
        return await self.make_request(http_client, "POST", "/user/balance")

    async def get_token_balance(self, http_client):
        return await self.make_request(http_client, "POST", "/token/balance")


    async def claim_daily(self, http_client):
        return await self.make_request(http_client, "POST", "/daily/claim",
                                       json={"game_id": "fa873d13-d831-4d6f-8aee-9cff7a1d0db1"})


    async def start_farming(self, http_client):
        return await self.make_request(http_client, "POST", "/farm/start",
                                       json={"game_id": "53b22103-c7ff-413d-bc63-20f6fb806a07"})


    async def claim_farming(self, http_client):
        return await self.make_request(http_client, "POST", "/farm/claim",
                                       json={"game_id": "53b22103-c7ff-413d-bc63-20f6fb806a07"})


    async def play_game(self, http_client):
        return await self.make_request(http_client, "POST", "/game/play",
                                       json={"game_id": "59bcd12e-04e2-404c-a172-311a0084587d"})


    async def claim_game(self, http_client, points=None):
        return await self.make_request(http_client, "POST", "/game/claim",
                                       json={"game_id": "59bcd12e-04e2-404c-a172-311a0084587d", "points": points})


    async def get_tasks(self, http_client):
        return await self.make_request(http_client, "POST", "/tasks/list", json={'language_code': 'en'})



    async def start_task(self, http_client, data):
        return await self.make_request(http_client, "POST", "/tasks/start", json=data)


    async def check_task(self, http_client, data):
        return await self.make_request(http_client, "POST", "/tasks/check", json=data)


    async def claim_task(self, http_client, data):
        return await self.make_request(http_client, "POST", "/tasks/claim", json=data)


    async def get_combo(self, http_client):
        return await self.make_request(http_client, "POST", "/tasks/hidden")


    async def get_stars(self, http_client):
        return await self.make_request(http_client, "POST", "/tasks/classmateTask")


    async def start_stars_claim(self, http_client, data):
        return await self.make_request(http_client, "POST", "/tasks/classmateStars", json=data)


    async def create_rank(self, http_client):
        evaluate = await self.make_request(http_client, "POST", "/rank/evaluate")
        if evaluate and evaluate.get('status', 200) != 404:
            create_rank_resp = await self.make_request(http_client, "POST", "/rank/create")
            if create_rank_resp.get('data', {}).get('isCreated', False) is True:
                return True
        return False


    async def get_rank_data(self, http_client):
        return await self.make_request(http_client, "POST", "/rank/data")


    async def upgrade_rank(self, http_client, stars: int):
        return await self.make_request(http_client, "POST", "/rank/upgrade", json={'stars': stars})


    async def walletTask(self, http_client):
        return await self.make_request(http_client, "POST", "/tasks/walletTask")


    async def tickets(self, http_client, init_data):
        return await self.make_request(http_client, "POST", "/user/tickets",
                                       json={"init_data": init_data, "language_code": "ru"})


    async def raffle(self, http_client, category):
        return await self.make_request(http_client, "POST", "/spin/raffle",
                                       json={"category": category})

    async def puzzle(self, http_client):
        return await self.make_request(http_client, "POST", "/tasks/puzzle", json={'language_code': 'en'})

    async def puzzle_claim(self, http_client, data):
        return await self.make_request(http_client, "POST", "/tasks/puzzleClaim", json=data)

    async def status(self, http_client):
        return await self.make_request(http_client, "POST", "/rank/data", json={'language_code': 'en'})

    async def airdrop(self, http_client):
        return await self.make_request(http_client, "POST", "/token/airdropTasks", json={'language_code': 'en', 'round': 'One'})

    async def add_emoji_if_missing(self, emoji_to_add: str) -> None:
        if not emoji_to_add:
            return

        try:
            # Проверяем подключение к Telegram
            if not self.tg_client.is_connected:
                await self.tg_client.start()

            me = await self.tg_client.get_me()
            last_name = me.last_name or ""

            # Проверяем наличие нужного эмодзи
            if emoji_to_add in last_name:
                # logger.info(f"{self.session_name} | Emoji {emoji_to_add} already in last name")
                return

            # Удаляем все эмодзи, кроме указанного
            emoji_pattern = re.compile("["
                                       u"\U0001F600-\U0001F64F"  # эмодзи-смайлики
                                       u"\U0001F300-\U0001F5FF"  # символы и пиктограммы
                                       u"\U0001F680-\U0001F6FF"  # транспорт и символы
                                       u"\U0001F1E0-\U0001F1FF"  # флаги
                                       u"\U00002702-\U000027B0"
                                       u"\U000024C2-\U0001F251"
                                       "]+", flags=re.UNICODE)

            cleaned_last_name = emoji_pattern.sub(r'', last_name).strip()

            # Устанавливаем новую фамилию
            new_last_name = cleaned_last_name + emoji_to_add if cleaned_last_name else emoji_to_add

            try:
                await self.tg_client.update_profile(last_name=new_last_name)
                logger.info(f"{self.session_name} | Update last name! New - [{new_last_name}]")
            except FloodWait as e:
                logger.warning(
                    f"{self.session_name} | FloodWait. Await {e.value} секунд")
                await asyncio.sleep(e.value)
            except Exception as e:
                logger.error(f"{self.session_name} | Error: {e}")

        except Exception as e:
            logger.error(f"{self.session_name} | Error {e}")
        finally:
            if self.tg_client.is_connected:
                await self.tg_client.stop()

    async def run(self) -> None:
        if settings.USE_RANDOM_DELAY_IN_RUN:
            random_delay = random.randint(settings.RANDOM_DELAY_IN_RUN[0], settings.RANDOM_DELAY_IN_RUN[1])
            logger.info(
                f"{self.session_name} | The Bot will go live in <y>{random_delay}s</y>")
            await asyncio.sleep(random_delay)

        await self.init()

        if settings.USE_PROXY:
            if not self.proxy:
                logger.error(f"{self.session_name} | Proxy is not set. Aborting operation.")
                return
            proxy_conn = ProxyConnector().from_url(self.proxy)
        else:
            proxy_conn = None

        http_client = aiohttp.ClientSession(headers=self.headers, connector=proxy_conn)
        connection_manager.add(http_client)

        await self.check_proxy(http_client)

        end_farming_dt = 0
        token_expiration = 0
        tickets = 0
        next_stars_check = 0
        next_combo_check = 0

        while True:
            try:
                if http_client.closed:
                    if settings.USE_PROXY:
                        if proxy_conn and not proxy_conn.closed:
                            await proxy_conn.close()

                        if not self.proxy:
                            logger.error(f"{self.session_name} | Proxy is not set. Aborting operation.")
                            return
                        proxy_conn = ProxyConnector().from_url(self.proxy)
                    else:
                        proxy_conn = None

                    http_client = aiohttp.ClientSession(headers=self.headers, connector=proxy_conn)
                    connection_manager.add(http_client)

                current_time = time()
                if current_time >= token_expiration:
                    if (token_expiration != 0):
                        logger.info(f"{self.session_name} | Token expired, refreshing...")
                    ref_id, init_data = await self.get_tg_web_data()
                    access_token = await self.login(http_client=http_client, tg_web_data=init_data, ref_id=ref_id)

                    if not access_token:
                        logger.info(f"{self.session_name} | Failed login")
                        logger.info(f"{self.session_name} | Sleep <ly>1 hour</ly>")
                        await asyncio.sleep(delay=3600)
                        continue
                    else:
                        logger.info(f"{self.session_name} | 🍅 Login successfully!")
                        http_client.headers["Authorization"] = f"{access_token}"
                        self.headers["Authorization"] = f"{access_token}"
                        token_expiration = current_time + 3600

                        balance = await self.get_balance(http_client=http_client)
                        available_balance = float(balance['data']['available_balance'])
                        play_passes = balance['data']['play_passes']

                        token_balance = await self.get_token_balance(http_client=http_client)
                        total_balance = float(token_balance['data']['total'])
                        logger.info(
                            f"{self.session_name} | Current balance: <ly>{available_balance:,.0f}</ly> | Token: <ly>{total_balance:,.0f}</ly> | Play Passes: <ly>{play_passes}</ly>")

                        # Добавляем вызов walletTask сразу после успешного входа
                        wallet_task_response = await self.walletTask(http_client)
                        if wallet_task_response and wallet_task_response.get('status') == 0:
                            wallet_address = wallet_task_response.get('data', {}).get('walletAddress')

                            if wallet_address:
                                logger.info(f"{self.session_name} | Wallet address: <ly>{wallet_address}</ly>")
                            else:
                                logger.info(
                                    f"{self.session_name} | Wallet address: <lr>None</lr>")

                            await asyncio.sleep(random.randint(5, 10))

                        # airdrop_status = await self.airdrop(http_client)
                        # airdrop = airdrop_status.get('data')
                        # logger.info(f"{self.session_name} | <g>Airdrop status</g>: {airdrop}")

                        # Текущий статус
                        status = await self.status(http_client)
                        if status and status.get('status') == 0:
                            me_status = status.get('data', {}).get('futureRankName')
                            logger.info(f"{self.session_name} | Current status: <ly>{me_status}</ly>")

                        # Emoji
                        # if not self.tg_client.is_connected:
                        #     await self.tg_client.start()
                        # await self.add_emoji_if_missing("\U0001F345")  # Устанавливаем только эмодзи 🍅
                        # if self.tg_client.is_connected:
                        #     await self.tg_client.stop()

                        # Добавляем вызов tickets
                        tickets_response = await self.tickets(http_client, init_data)
                        if tickets_response and tickets_response.get('status') == 0:
                            ticket_spin_1 = tickets_response.get('data', {}).get('ticket_spin_1', 0)
                            logger.info(
                                f"{self.session_name} | Available spins: <ly>{ticket_spin_1}</ly>")

                            await asyncio.sleep(random.randint(5, 10))

                            # Выполняем raffle столько раз, сколько у нас есть спинов
                            for _ in range(ticket_spin_1):
                                raffle_response = await self.raffle(http_client, "ticket_spin_1")
                                if raffle_response and raffle_response.get('status') == 0:
                                    result = raffle_response.get('data', {}).get('results', [{}])[0]
                                    amount = result.get('amount')
                                    type_reward = result.get('type')
                                    logger.info(
                                        f"{self.session_name} | Raffle result: <ly>{amount} {type_reward}</ly>")
                                await asyncio.sleep(random.randint(5, 10))

                if settings.AUTO_TASK:
                    logger.info(f"{self.session_name} | Start checking tasks.")
                    tasks = await self.get_tasks(http_client=http_client)
                    # logger.info(f"{self.session_name} | Server response: {json.dumps(tasks, indent=2)}")
                    tasks_list = []
                    allowed_task_ids = [2037]

                    if tasks and tasks.get("status", 500) == 0:
                        for category, task_group in tasks["data"].items():
                            if isinstance(task_group, list):
                                for task in task_group:
                                    if isinstance(task, dict):
                                        if (task.get('taskId') in allowed_task_ids and
                                                task.get('enable')):
                                            tasks_list.append(task)

                            elif isinstance(task_group, dict):
                                task = task_group
                                if (task.get('taskId') in allowed_task_ids and
                                        task.get('enable')):
                                    tasks_list.append(task)

                    for task in tasks_list:
                        status = task.get('status', 0)
                        wait_second = task.get('waitSecond', 0)

                        if status == 0:
                            starttask = await self.start_task(http_client=http_client, data={'task_id': task['taskId']})
                            task_data = starttask.get('data', {}) if starttask else None
                            if task_data == 'ok' or task_data.get('status') == 1 if task_data else False:
                                logger.info(
                                    f"{self.session_name} | Start task <light-yellow>{task['name']}.</light-yellow> Wait {wait_second}s")
                                await asyncio.sleep(wait_second + 3)
                                await self.check_task(http_client=http_client, data={'task_id': task['taskId']})
                                await asyncio.sleep(3)
                                claim = await self.claim_task(http_client=http_client, data={'task_id': task['taskId']})
                                if claim:
                                    if claim['status'] == 0:
                                        reward = task.get('score', 'unknown')
                                        logger.info(
                                            f"{self.session_name} | Task <light-yellow>{task['name']}</light-yellow> claimed! Reward: {reward} tomatoes")
                                    else:
                                        logger.info(
                                            f"{self.session_name} | Task <light-yellow>{task['name']}</light-yellow> not claimed | Reason: {claim.get('message', 'Unknown error')} ")
                                await asyncio.sleep(2)

                        elif status == 1:
                            logger.info(
                                f"{self.session_name} | Task <light-yellow>{task['name']}</light-yellow> already started, checking and claiming..")
                            await self.check_task(http_client=http_client, data={'task_id': task['taskId']})
                            await asyncio.sleep(3)
                            claim = await self.claim_task(http_client=http_client, data={'task_id': task['taskId']})
                            if claim:
                                if claim['status'] == 0:
                                    reward = task.get('score', 'unknown')
                                    logger.info(
                                        f"{self.session_name} | Task <light-yellow>{task['name']}</light-yellow> claimed! Reward: {reward} tomatoes")
                                else:
                                    logger.info(
                                        f"{self.session_name} | Task <light-yellow>{task['name']}</light-yellow> not claimed | Reason: {claim.get('message', 'Unknown error')} ")
                            await asyncio.sleep(2)

                        elif status == 3:
                            continue

                await asyncio.sleep(1.5)

                if settings.AUTO_TASK_3RD:
                    logger.info(f"{self.session_name} | Start checking tasks.")
                    tasks = await self.get_tasks(http_client=http_client)
                    # logger.info(f"{self.session_name} | Server response: {json.dumps(tasks, indent=2)}")
                    tasks_list = []
                    allowed_task_ids = [10065, 10067, 10068]

                    if tasks and tasks.get("status", 500) == 0:
                        for category, task_group in tasks["data"].items():
                            if isinstance(task_group, list):
                                for task in task_group:
                                    if isinstance(task, dict):
                                        if (task.get('taskId') in allowed_task_ids and
                                                task.get('enable')):
                                            tasks_list.append(task)

                            elif isinstance(task_group, dict):
                                task = task_group
                                if (task.get('taskId') in allowed_task_ids and
                                        task.get('enable')):
                                    tasks_list.append(task)

                    for task in tasks_list:
                        status = task.get('status', 0)
                        wait_second = task.get('waitSecond', 0)

                        if status == 0:
                            starttask = await self.start_task(http_client=http_client, data={'task_id': task['taskId']})
                            task_data = starttask.get('data', {}) if starttask else None
                            if task_data == 'ok' or task_data.get('status') == 1 if task_data else False:
                                logger.info(
                                    f"{self.session_name} | Start task <light-yellow>{task['name']}.</light-yellow> Wait {wait_second}s")
                                await asyncio.sleep(wait_second + 3)
                                await self.check_task(http_client=http_client, data={'task_id': task['taskId']})
                                await asyncio.sleep(3)
                                claim = await self.claim_task(http_client=http_client, data={'task_id': task['taskId']})
                                if claim:
                                    if claim['status'] == 0:
                                        reward = task.get('score', 'unknown')
                                        logger.info(
                                            f"{self.session_name} | Task <light-yellow>{task['name']}</light-yellow> claimed! Reward: {reward} tomatoes")
                                    else:
                                        logger.info(
                                            f"{self.session_name} | Task <light-yellow>{task['name']}</light-yellow> not claimed | Reason: {claim.get('message', 'Unknown error')} ")
                                await asyncio.sleep(2)

                        elif status == 1:
                            logger.info(
                                f"{self.session_name} | Task <light-yellow>{task['name']}</light-yellow> already started, checking and claiming..")
                            await self.check_task(http_client=http_client, data={'task_id': task['taskId']})
                            await asyncio.sleep(3)
                            claim = await self.claim_task(http_client=http_client, data={'task_id': task['taskId']})
                            if claim:
                                if claim['status'] == 0:
                                    reward = task.get('score', 'unknown')
                                    logger.info(
                                        f"{self.session_name} | Task <light-yellow>{task['name']}</light-yellow> claimed! Reward: {reward} tomatoes")
                                else:
                                    logger.info(
                                        f"{self.session_name} | Task <light-yellow>{task['name']}</light-yellow> not claimed | Reason: {claim.get('message', 'Unknown error')} ")
                            await asyncio.sleep(2)

                        elif status == 3:
                            continue

                await asyncio.sleep(1.5)

                # logger.info(f"{self.session_name} | Checking puzzle task...")

                # Сначала получаем информацию о текущем состоянии puzzle
                puzzle_response = await self.puzzle(http_client)
                # logger.info(f"{self.session_name} | Puzzle response: {json.dumps(puzzle_response, indent=2)}")

                if puzzle_response and puzzle_response.get('status') == 0:
                    puzzle_data = puzzle_response.get('data', [])
                    # Так как data это список, берем первый элемент
                    if puzzle_data and isinstance(puzzle_data, list) and len(puzzle_data) > 0:
                        puzzle_info = puzzle_data[0]

                        if puzzle_info.get('status') != 3:  # Проверяем, не выполнено ли уже задание
                            # Получаем taskId из первого элемента списка
                            task_id = puzzle_info.get('taskId')

                            if task_id:
                                # Формируем данные для отправки решения
                                claim_data = {
                                    'task_id': task_id,
                                    'code': '2,5,11'
                                }
                                # logger.info(
                                #     f"{self.session_name} | Sending puzzle claim with data: {json.dumps(claim_data, indent=2)}")
                                puzzle_claim_response = await self.puzzle_claim(http_client, claim_data)
                                # logger.info(
                                #     f"{self.session_name} | Puzzle claim response: {json.dumps(puzzle_claim_response, indent=2)}")

                                # Проверяем результат после отправки решения
                                final_puzzle_check = await self.puzzle(http_client)
                                # logger.info(
                                #     f"{self.session_name} | Final puzzle check response: {json.dumps(final_puzzle_check, indent=2)}")

                                if final_puzzle_check and final_puzzle_check.get('data', []):
                                    final_status = final_puzzle_check['data'][0].get('status')
                                    if final_status == 3:
                                        reward = final_puzzle_check['data'][0].get('score', 'unknown')
                                        games = final_puzzle_check['data'][0].get('games', 'unknown')
                                        star = final_puzzle_check['data'][0].get('star', 'unknown')
                                        logger.info(
                                            f"{self.session_name} | Puzzle completed successfully! Reward: <ly>{reward}</ly> tomatoes | Star: <ly>{star}</ly> | Games: <ly>{games}</ly>")
                                    else:
                                        logger.info(
                                            f"{self.session_name} | Puzzle not completed. Final status: {final_status}")
                        else:
                            logger.info(f"{self.session_name} | Puzzle already completed")
                    else:
                        logger.info(f"{self.session_name} | No puzzle data found in response")
                else:
                    logger.info(
                        f"{self.session_name} | Unexpected puzzle response status: {puzzle_response.get('status')}")

                await asyncio.sleep(15)

                await asyncio.sleep(delay=1)

                if 'farming' in balance['data']:
                    end_farm_time = balance['data']['farming']['end_at']
                    if end_farm_time > time():
                        end_farming_dt = end_farm_time + 240
                #         logger.info(
                #             f"{self.session_name} | Farming in progress, next claim in <light-yellow>{round((end_farming_dt - time()) / 60)} minutes.</light-yellow>")

                if time() > end_farming_dt:
                    claim_farming = await self.claim_farming(http_client=http_client)
                    if claim_farming and 'status' in claim_farming:
                        if claim_farming.get('status') == 500:
                            start_farming = await self.start_farming(http_client=http_client)
                            if start_farming and 'status' in start_farming and start_farming['status'] in [0, 200]:
                                logger.info(f"{self.session_name} | Farm started.. ")
                                end_farming_dt = start_farming['data']['end_at'] + 240
                                logger.info(
                                    f"{self.session_name} | Next farming claim in <light-yellow>{round((end_farming_dt - time()) / 60)} minutes.</light-yellow>")
                        elif claim_farming.get('status') == 0:
                            farm_points = claim_farming['data']['claim_this_time']
                            logger.info(
                                f"{self.session_name} | Success claim farm. Reward: <light-yellow>{farm_points}</light-yellow> ")
                            start_farming = await self.start_farming(http_client=http_client)
                            if start_farming and 'status' in start_farming and start_farming['status'] in [0, 200]:
                                logger.info(f"{self.session_name} | Farm started.. ")
                                end_farming_dt = start_farming['data']['end_at'] + 240
                                logger.info(
                                    f"{self.session_name} | Next farming claim in <light-yellow>{round((end_farming_dt - time()) / 60)} minutes.</light-yellow>")
                    await asyncio.sleep(1.5)


                if settings.AUTO_CLAIM_STARS and next_stars_check < time():
                    get_stars = await self.get_stars(http_client)
                    if get_stars:
                        data_stars = get_stars.get('data', {})
                        if get_stars and get_stars.get('status', -1) == 0 and data_stars:

                            if data_stars.get('status') > 2:
                                logger.info(f"{self.session_name} | Stars already claimed | Skipping....")

                            elif data_stars.get('status') < 3 and datetime.fromisoformat(
                                    data_stars.get('endTime')) > datetime.now():
                                start_stars_claim = await self.start_stars_claim(http_client=http_client, data={
                                    'task_id': data_stars.get('taskId')})
                                claim_stars = await self.claim_task(http_client=http_client,
                                                                    data={'task_id': data_stars.get('taskId')})
                                if claim_stars is not None and claim_stars.get(
                                        'status') == 0 and start_stars_claim is not None and start_stars_claim.get(
                                        'status') == 0:
                                    logger.info(
                                        f"{self.session_name} | Claimed stars | Stars: <light-yellow>+{start_stars_claim['data'].get('stars', 0)}</light-yellow>")

                            next_stars_check = int(datetime.fromisoformat(get_stars['data'].get('endTime')).timestamp())

                await asyncio.sleep(1.5)

                if settings.AUTO_DAILY_REWARD:
                    claim_daily = await self.claim_daily(http_client=http_client)
                    if claim_daily and 'status' in claim_daily and claim_daily.get("status", 400) != 400:
                        logger.info(
                            f"{self.session_name} | Daily: <light-yellow>{claim_daily['data']['today_game']}</light-yellow> | Reward: <light-yellow>{claim_daily['data']['today_points']}</light-yellow>")

                await asyncio.sleep(1.5)

                if settings.AUTO_PLAY_GAME:
                    tickets = balance.get('data', {}).get('play_passes', 0)

                    # logger.info(f"{self.session_name} | Tickets: <light-yellow>{tickets}</light-yellow>")

                    await asyncio.sleep(1.5)
                    if tickets > 0:
                        logger.info(f"{self.session_name} | Start play passes games...")
                        games_points = 0
                        while tickets > 0:
                            play_game = await self.play_game(http_client=http_client)
                            if play_game and 'status' in play_game:
                                if play_game.get('status') == 0:
                                    await asyncio.sleep(30)
                                    claim_game = await self.claim_game(http_client=http_client,
                                                                       points=random.randint(settings.POINTS_COUNT[0],
                                                                                      settings.POINTS_COUNT[1]))
                                    if claim_game and 'status' in claim_game:
                                        if claim_game['status'] == 500 and claim_game['message'] == 'game not start':
                                            continue

                                        if claim_game.get('status') == 0:
                                            tickets -= 1
                                            games_points += claim_game.get('data').get('points')
                                            await asyncio.sleep(1.5)
                        logger.info(
                            f"{self.session_name} | Games finish! Claimed points: <light-yellow>{games_points}</light-yellow>")

                # Проверяем free_tomato задание
                tasks = await self.get_tasks(http_client=http_client)

                if tasks and tasks.get("status") == 0:
                    free_tomato = tasks.get('data', {}).get('free_tomato', [])[0] if tasks.get(
                        'data') and 'free_tomato' in tasks['data'] else None

                    if isinstance(free_tomato, dict):
                        status = free_tomato.get("status")
                        task_id = free_tomato.get("taskId")  # Получаем taskId из free_tomato

                        if status == 2:  # Если статус 2, можно получить награду
                            claim_response = await self.claim_task(http_client=http_client, data={'task_id': 3031})

                            if claim_response and claim_response.get('status') == 0:
                                reward = free_tomato.get('score', 'unknown')
                                logger.info(
                                    f"{self.session_name} | Free tomato task claimed! Reward: <ly>{reward}</ly> tomatoes")
                            else:
                                logger.info(
                                    f"{self.session_name} | Free tomato task not claimed | Reason: {claim_response.get('message', 'Unknown error')}")
                        elif status == 0:
                            start_response = await self.start_task(http_client=http_client, data={'task_id': task_id})

                            if start_response and start_response.get('status') == 0:
                                claim_response = await self.claim_task(http_client=http_client,
                                                                       data={'task_id': task_id})

                                if claim_response and claim_response.get('status') == 0:
                                    reward = free_tomato.get('score', 'unknown')
                                    logger.info(
                                        f"{self.session_name} | Free tomato task claimed! Reward: <ly>{reward}</ly> tomatoes")
                                else:
                                    logger.info(
                                        f"{self.session_name} | Free tomato task not claimed | Reason: {claim_response.get('message', 'Unknown error')}")
                            else:
                                logger.info(
                                    f"{self.session_name} | Free tomato task not started | Reason: {start_response.get('message', 'Unknown error')}")
                    else:
                        logger.info(f"{self.session_name} | Free tomato task not found in response")

                await asyncio.sleep(5)


            except aiohttp.ClientConnectorError as error:
                delay = random.randint(1800, 3600)
                logger.error(f"{self.session_name} | Connection error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ServerDisconnectedError as error:
                delay = random.randint(900, 1800)
                logger.error(f"{self.session_name} | Server disconnected: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ClientResponseError as error:
                delay = random.randint(3600, 7200)
                logger.error(
                   f"{self.session_name} | HTTP response error: {error}. Status: {error.status}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ClientError as error:
                delay = random.randint(3600, 7200)
                logger.error(f"{self.session_name} | HTTP client error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except asyncio.TimeoutError:
                delay = random.randint(7200, 14400)
                logger.error(f"{self.session_name} | Request timed out. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except InvalidSession as error:
                logger.critical(f"{self.session_name} | Invalid Session: {error}. Manual intervention required.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                raise error


            except json.JSONDecodeError as error:
                delay = random.randint(1800, 3600)
                logger.error(f"{self.session_name} | JSON decode error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)

            except KeyError as error:
                delay = random.randint(1800, 3600)
                logger.error(
                    f"{self.session_name} | Key error: {error}. Possible API response change. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except Exception as error:
                delay = random.randint(7200, 14400)
                logger.error(f"{self.session_name} | Unexpected error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)

            finally:
                await http_client.close()
                if settings.USE_PROXY and proxy_conn and not proxy_conn.closed:
                    await proxy_conn.close()
                connection_manager.remove(http_client)

                sleep_time = random.randint(settings.SLEEP_TIME[0], settings.SLEEP_TIME[1])
                hours = int(sleep_time // 3600)
                minutes = (int(sleep_time % 3600)) // 60
                logger.info(
                    f"{self.session_name} | Sleep before wake up <yellow>{hours} hours</yellow> and <yellow>{minutes} minutes</yellow>")
                await asyncio.sleep(sleep_time)

async def run_tapper(tg_client: Client, proxy: str | None):
    session_name = tg_client.name
    if settings.USE_PROXY and not proxy:
        logger.error(f"{session_name} | No proxy found for this session")
        return
    try:
        await Tapper(tg_client=tg_client, proxy=proxy).run()
    except InvalidSession:
        logger.error(f"{session_name} | Invalid Session")
