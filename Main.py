import threading
import time
import Updater as up
import Processor as proc
import Database as db


def get_api_url(url):
    prefix = "https://api.onegov.nsw.gov.au"
    return f"{prefix}{url}"


def get_api_dict():
    return {"auth": get_api_url("/oauth/client_credential/accesstoken"),
            "ref": get_api_url("/FuelCheckRefData/v2/fuel/lovs"),
            "cur": get_api_url("/FuelPriceCheck/v2/fuel/prices"),
            "new": get_api_url("/FuelPriceCheck/v2/fuel/prices/new"),
            "stn": get_api_url("/FuelPriceCheck/v2/fuel/prices/station/"),
            "loc": get_api_url("/FuelPriceCheck/v2/fuel/prices/location"),
            "rad": get_api_url("/FuelPriceCheck/v2/fuel/prices/nearby")}


def update_thread(upd: up.Updater):
    print("Update Thread Started...")
    while True:
        time.sleep(10800)
        upd.update("new")


if __name__ == '__main__':
    proc.Processor()
    db.Database()
    updater = up.Updater(get_api_dict())
    updater.update("cur")
    threading.Thread(target=update_thread, args=(updater,)).start()
