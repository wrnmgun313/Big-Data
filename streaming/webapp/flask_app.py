"""
    This Flask web app provides a very simple dashboard to visualize the statistics sent by the spark app.
    The web app is listening on port 5000.
    All apps are designed to be run in Docker containers.
    
    Made for: EECS 4415 - Big Data Systems (Department of Electrical Engineering and Computer Science, York University)
    Author: Changyuan Lin

"""
import datetime

from flask import Flask, jsonify, request, render_template
import matplotlib.dates as mdates

from redis import Redis
import matplotlib.pyplot as plt
import json

TOP10 = "top10"
TOTAL_COUNT_IN_60S = "total_count_in_60s"
TOTAL_ACOUNT = "total_count"
LANGUAGES = ['Python', 'JavaScript', 'Java']
AVG_STARS = "avg_stars"

app = Flask(__name__)


@app.route('/updateData', methods=['POST'])
def updateData():
    package = request.get_json()
    key = package['key']
    time = package['time']
    data = package['data']
    print("update")

    r = Redis(host='redis', port=6379)
    if key == TOTAL_COUNT_IN_60S:
        for l in data:
            l['time'] = time
        r.lpush(key, json.dumps(data))
    else:
        r.set(key, json.dumps(data))
    return jsonify({'msg': 'success'})


def draw_avg_stars(data):
    langs = []
    avg_stars = []
    for line in data:
        if line['language'] not in LANGUAGES:
            continue
        langs.append(line['language'])
        avg_stars.append(line['avg_star'])
    print(langs)
    print(avg_stars)
    fig, host = plt.subplots()
    host.bar(range(len(avg_stars)), avg_stars, color=["red", "green", "blue"], tick_label=langs)
    plt.xlabel("PL")
    plt.ylabel("Average number of stars")
    plt.savefig('/streaming/webapp/static/images/stars.png')
    plt.show()


def draw_count_in_60s(data):
    cnt_list = []
    for line in data:
        line = json.loads(line)
        cnt_list.append(line)

    fig, host = plt.subplots()
    host.grid(False)
    host.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    for lang in LANGUAGES:
        a_lang_list = []
        for l in cnt_list:
            a_lang_list.extend([line for line in l if line['language'] == lang])
        a_lang_list.sort(key=lambda x: x['time'])
        times = [datetime.datetime.strptime(x['time'], "%Y-%m-%d %H:%M:%S") for x in a_lang_list]
        y = [x['count'] for x in a_lang_list]
        # plt.tight_layout()
        print(times)
        print(y)
        host.plot(times, y, label=lang)
        # host.scatter(times, y)
    plt.ylabel('#repositories')
    plt.xlabel('Time')
    plt.legend(loc='best', fontsize=8)  # 标签位置
    plt.savefig('/streaming/webapp/static/images/60s.png')


def show_top10():
    result = {}
    r = Redis(host='redis', port=6379)
    data = r.get(TOP10)
    try:
        data = json.loads(data)
    except TypeError:
        return "waiting for data..."

    for lang in LANGUAGES:
        result[lang] = []
        a_lang_list = []
        for line in data:
            if line['language'] == lang:
                a_lang_list.append(line)
        a_lang_list.sort(key=lambda x: x['count'], reverse=True)
        for line in a_lang_list:
            result[lang].append(f"{line['word']}, {line['count']}")
    return result


@app.route('/', methods=['GET'])
def index():
    r = Redis(host='redis', port=6379)
    data = r.get(TOTAL_ACOUNT)
    try:
        data = json.loads(data)
    except TypeError:
        return "waiting for data..."
    print("Total number of collected repositories")

    total = data
    print(total)
    print("Number of the collected repositories with changes pushed during the last 60 seconds")

    size = r.llen(TOTAL_COUNT_IN_60S)
    print(size)
    data = r.lrange(TOTAL_COUNT_IN_60S, 0, size)
    try:
        draw_count_in_60s(data)
    except TypeError:
        return "waiting for data..."

    data = r.get(AVG_STARS)
    try:
        data = json.loads(data)
        draw_avg_stars(data)
    except TypeError:
        return "waiting for data..."

    top10 = show_top10()
    print(top10)
    return render_template('index.html', url_60s='./static/images/60s.png', total=total,
                           stars="./static/images/stars.png", top10=top10)


if __name__ == '__main__':
    # r = Redis(host='redis', port=6379)
    # data = r.get(AVG_STARS)
    # data = json.loads(data)
    # draw_avg_stars(data)
    app.debug = True
    app.run(host='0.0.0.0')
