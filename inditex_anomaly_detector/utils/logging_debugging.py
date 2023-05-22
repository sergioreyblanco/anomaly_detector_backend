import logging
import timeit
import pandas as pd


def common_print(title, body):
    body=str(body)
    pd.set_option('display.max_columns', 100)
    pd.set_option('display.max_rows', 100)
    if title != "":
        print("\n"+title + ": " + body)
        logging.info(title + ": " + body)
    else:
        print("\n"+body)
        logging.info(body)


def start_chrono():
    start=timeit.default_timer()

    return start


def end_chrono(title, start):
    end = timeit.default_timer()
    common_print("\n\n\t * Time used for "+title, str(round((end - start), 2)) + ' secs\n\n')