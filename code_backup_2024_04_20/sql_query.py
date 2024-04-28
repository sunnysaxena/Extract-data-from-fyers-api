import time

init = 40
count = 1


def loop_forever():
    global init, count
    while True:

        try:
            if count == init:
                time.sleep(8)
                print('\n')
                init += 40

            count += 1
            print(count, init)
        except Exception as e:
            print('Hello', e)


from datetime import datetime

print(str(datetime.now().time()) + '.csv')
