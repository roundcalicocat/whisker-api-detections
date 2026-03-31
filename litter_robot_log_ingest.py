import asyncio
from getpass import getpass

from pylitterbot import Account

username = getpass("Username:")
password = getpass("Password:")


async def main():
    account = Account()

    try:
        await account.connect(username=username, password=password, load_robots=True)

        for robot in account.robots:
            print(robot)
            for activity in await robot.get_activity_history(100):
                print(activity)
    finally:
        await account.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
    