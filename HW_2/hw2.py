import time
import uuid
import pandas as pd
from requests.exceptions import ConnectionError
from amplitude_python_sdk.v2.clients.event_client import EventAPIClient
from amplitude_python_sdk.v2.models.event import Event
from amplitude_python_sdk.v2.models.event.options import EventAPIOptions


# время старта текущего экземпляра скрипта
start_time = time.time()

# наши ключи
api_key = 'eb2754abdafd13e0a093888165b14759'

# генерируемый идентификатор
denerate_id = str(uuid.uuid4())

# позаботимся о том, чтобы можно было локально хранить информацию
f = open('users_info.csv','a+')
f.close()

try:
  frame = pd.read_csv('users_info.csv', delimiter=',')
except:
  frame = pd.read_csv('users_info.csv', delimiter=',',
                      names=['ident', 'start_time', 'end_time', 'is_pub'])


# функция, которая отправялет данные на сервер
def send_events(client, denerate_id, start_time, new_time):

    events = [
        Event(
            user_id=denerate_id,
            event_type='Sending information',
            session_id=start_time,
            event_properties={
                'user_id': denerate_id,
                'start_session': start_time,
                'duration': new_time,
            }
        )
    ]
    client.upload(
        events=events,
        options=EventAPIOptions(min_id_length=1),
    )


# функция, которая будет загружать информацию, которая не была отправлена из-за отсутсвия сигнала
def send_info_nowf(client):
    try:
        new_df = pd.read_csv('users_info.csv', delimiter=',')
        for ind, row in new_df.iterrows():
            if row[3] == 'no':
                send_events(client, row[0], row[1], row[2])
        return True
    except:
        return False


# если сломается соединение, все будет записано тут
def for_exception(frame=frame):
    try:
        while True:
            new_time = time.time() - start_time
            data = {'ident': denerate_id,
                    'start_time': start_time,
                    'end_time': new_time,
                    'is_pub': 'no'}
            """
            могло случиться так, что событие уже было отправлено, однако
            из-за остановки скрипта может быть создана запись о собтии еше раз
            в таком случае будем это регулировать:
            создаем массив для того, чтобы проверялось, было ли уже отправлено событие
            """
            extra_row = []

            for ind, row in frame.iterrows():

                if (row[0] == data['ident'] and row[1] == data['start_time'] and row[2] == data['end_time'] \
                        and row[3] == 'yes'):
                    extra_row.append(row.tolist())

            # если событие не было отправлено, то фиксируем информацию об этом
            if len(extra_row) < 1:
                frame = frame.append(data, ignore_index=True)
                frame.to_csv('users_info.csv', index=False)

                # делаем так, что события отправляются раз в 5 секунд
                time.sleep(5)
    except:
        pass


try:
    client = EventAPIClient(api_key=api_key)

    # сначала запишем то, что могло не записаться ранее
    if send_info_nowf(client) is True:
        # так как мы обрабатываем все случаи (отправляем на сервер все), просто меняем значения, чтобы больше не путаться
        frame['is_pub'] = frame['is_pub'].replace(to_replace='no', value='yes')
        frame.to_csv('users_info.csv', index=False)

    while True:

        # время, прошедшее со старта текущего экземпляра скрипта
        new_time = time.time() - start_time

        send_events(client, denerate_id, start_time, new_time)

        # если все ок, то записываем это в файл с параметром yes
        data = {'ident': denerate_id,
                'start_time': start_time,
                'end_time': new_time,
                'is_pub': 'yes'}

        frame = frame.append(data, ignore_index=True)
        frame.to_csv('users_info.csv', index=False)

        # делаем так, что события отправляются раз в 5 секунд
        time.sleep(5)


except ConnectionError:
    # чтобы у нас потом не появлялось куча ненужных ошибок, положим все в отдельную функцию
    for_exception()

except:
    pass
