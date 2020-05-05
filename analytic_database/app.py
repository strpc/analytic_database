# -*- coding: utf-8 -*-
import asyncio
import uvloop

from time import sleep

import dwr_service
import logger
import service
from request_database import Request, Device
from config import (PG_USER,
                    PG_PASSWORD,
                    PG_HOST,
                    PG_DB,
                    DEVICE_WITHOUT_RECEIPTS,
                    TIME_SLEEP
                    )


DATE_START = '2020-01-01'
DATE_FINISH = '2020-01-02'


if __name__ == '__main__':
    request = Request(pg_user=PG_USER,
                      pg_password=PG_PASSWORD,
                      pg_host=PG_HOST,
                      pg_db=PG_DB,
                      date_start=DATE_START,  # dev
                      date_finish=DATE_FINISH
                      )
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    while True:
        # from time import time #dev
        # t1 = time()
        asyncio.run(service.run_app(request))
        if DEVICE_WITHOUT_RECEIPTS:
            asyncio.run(dwr_service.run_app(request))
        sleep(TIME_SLEEP)

        # print(f"Passed: {round(time() - t1, 2)}")
