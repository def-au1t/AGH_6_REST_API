import asyncio
import platform
from datetime import datetime
import threading
import time
from functools import wraps

import aiohttp as aiohttp
import requests as requests
from aiohttp import ClientSession, ClientConnectorError
from flask import Flask, jsonify, render_template, request

from asyncio.proactor_events import _ProactorBasePipeTransport


start_time: float = 0.0
# FIX FOR BUG IN ASYNCIO LIBRARY IN WINDOWS:

def silence_event_loop_closed(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except RuntimeError as e:
            if str(e) != 'Event loop is closed':
                raise

    return wrapper

if platform.system() == 'Windows':
    _ProactorBasePipeTransport.__del__ = silence_event_loop_closed(_ProactorBasePipeTransport.__del__)

# ----

async def fetch_html(url: str, session: ClientSession) -> {}:
    resp = {}
    try:
        print(f"Query start: {time.time() - start_time : .3f}")
        resp = await session.get(url)
        if resp.status == 200:
            print(f"Query response: {time.time() - start_time : .3f}")
            json = await resp.json()
            return json
        elif resp.status == 503:
            for trial in range(10):
                time.sleep(0.1)
                print(f"API throttling: Try {trial+2}")
                resp = await session.get(url)
                if resp.status == 200:
                    print(f"Query response {trial+2}: {time.time() - start_time : .3f}")
                    json = await resp.json()
                    return json
        else: return None
    except Exception as e:
        print(e)
        return None


def create_app():
    app = Flask(__name__)

    async def get_object_data(objects: []):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for object in objects:
                url = f'https://ssd-api.jpl.nasa.gov/sentry.api?des={object}'
                tasks.append(fetch_html(url=url, session=session))
            res = await asyncio.gather(*tasks)
            res = [el for el in res if el is not None]
            return res

    def get_summary(object: {}, from_date: datetime, to_date: datetime):
        try:
            res = {}
            res['name'] = object['summary']['fullname']
            data = object['data']
            prob = 1
            for meeting in data:
                prob = prob * (1 - float(meeting['ip']))
            prob = 1 - prob
            res['total_100_prob'] = prob * 100
            res['total_100_times'] = len(data)
            data = [obj for obj in data if
                    (from_date <= datetime.strptime(obj['date'].split(".")[0], '%Y-%m-%d') <= to_date)]
            res['nearby_count'] = len(data)
            data.sort(key=lambda x: float(x['ip']), reverse=True)
            res['max_prob'] = float(data[0]['ip']) * 100
            if 'dist' in data[0]:
                res['dist'] = float(data[0]['dist'])*6420
            else:
                res['dist'] = float(0)
            if 'date' in data[0]:
                res['date'] = datetime.strptime(data[0]['date'].split(".")[0], '%Y-%m-%d')
            else:
                res['date'] = datetime.date(2100, 1, 1)
            if 'diameter' in object['summary']:
                res['diameter'] = float(object['summary']['diameter'])*1000
            else:
                res['diameter'] = float(0)
        except Exception as e:
            print(e)
            return res
        return res


    @app.route("/", methods=["GET", "POST"])
    def index():
        if request.method == "GET":
            return render_template('base.html')
        elif request.method == "POST":
            global start_time
            start_time = time.time()
            form_data = request.form
            from_date = datetime.strptime(form_data['from'], '%Y-%m-%d')
            to_date = datetime.strptime(form_data['to'], '%Y-%m-%d')
            min_prob = 10 ** (int(form_data['prob']))
            limit = int(form_data['limit'])
            start_time = time.time()
            nasa_resp = {}
            try:
                nasa_resp = requests.get(f"https://ssd-api.jpl.nasa.gov/sentry.api?all=1&ip-min={min_prob}")
            except Exception as e:
                print(e)
                return render_template('base.html', message="Wystąpił błąd z połączeniem do API NASA")
            if nasa_resp.status_code != 200:
                return render_template('base.html', message="Wystąpił błąd po stronie API NASA")
            resp_json: dict = nasa_resp.json()
            if 'data' not in resp_json:
                return render_template('base.html', message="Wystąpił błąd po stronie API NASA")
            objects_arr: [] = resp_json['data']
            objects_filtered = []
            for object in objects_arr:
                if 'ip' not in object:
                    return render_template('base.html', message="Wystąpił błąd po stronie API NASA")
                try:
                    obj_date = datetime.strptime(object['date'].split(".")[0], '%Y-%m-%d')
                except ValueError:
                    print(f"Improper date: {object['date']}")
                    continue
                if (from_date <= obj_date <= to_date) and float(object['ip']) > min_prob:
                    objects_filtered.append(object)
            objects_filtered.sort(key=lambda o: float(o["ip"]), reverse=True)
            object_names = set()
            for o in objects_filtered:
                object_names.add(o['des'])
                if len(object_names) >= limit: break

            asyncio.set_event_loop(asyncio.new_event_loop())
            loop = asyncio.get_event_loop()
            obj_detailed_results = loop.run_until_complete(get_object_data(list(object_names)))
            loop.close()
            obj_summary = [get_summary(o, from_date, to_date) for o in obj_detailed_results]
            obj_summary.sort(key=lambda x: x['max_prob'], reverse=True)
            print(f"Total time: {time.time() - start_time: .2f} s. ")
            
            return render_template('result.html', data=obj_summary)

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0')
