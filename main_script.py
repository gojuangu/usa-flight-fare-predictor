import argparse
from d0_gathering import gathering as dga
from d1_transformation import transformation as trn
from d2_model import model as mod
from d3_check import check_flight as chk


def argument_parser():
    parser = argparse.ArgumentParser(description='Set path')
    parser.add_argument('-tp', "--downloads_path", type=str, help='specify where you have download the zipped data'
                        , required=True)
    parser.add_argument('-p', "--path", type=str, help='specify where to store the unzipped data for the model'
                        , required=True)
    args = parser.parse_args()
    return args

# Expect around 30 minutes to run all the pipeline, recommended only to run the first time
def pipeline(arguments):
    print('========================= Pipeline is starting! =========================')
    dga.unzipp(arguments.downloads_path, arguments.path)  # unzip files
    spark = dga.sparkbuilder()  # build the Spark Session
    df_t = dga.spark_parquet_ticket(spark, arguments.path)  # gather all info converting it to parquet
    df_c = dga.spark_parquet_coupon(spark, arguments.path)
    transform_tickets = trn.tickets_transformation(df_t)  # transform all info ready to model it
    transform_coupons = trn.coupons_transformation(df_c)
    trn.join_coupons_tickets(spark, transform_coupons, transform_tickets, arguments.path)
    mod.data_partition(arguments.path)
    mod.model(arguments.path)

    print('========================= Pipeline is complete! =========================\n'
          'Do you want to make a consultation?')
    while True:
        question_2 = (input('[y]Yes / [n]No: '))
        if question_2.lower() not in ('y', 'n'):
            print(f'Sorry, {question_2} is not a correct answer.')
        else:
            break

    if question_2 == 'y':
        print('Great, please, answer the following questions:')
        chk.check_flight(arguments.path)
    else:
        print('Thanks for your time :)')

def consult(arguments):
    chk.check_flight(arguments.path)


if __name__ == '__main__':
    arguments = argument_parser()

    print(
        'Welcome to the USA Fare Flight predictor. Please select if you want to start the pipeline os just make a consultation:')

    while True:
        answer = (input('[c]Consult or [p]Pipeline: '))
        if answer.lower() not in ('p', 'c'):
            print(f'Sorry, {answer} is not a correct choice. Please, select c or p')
        else:
            break

    if answer == 'c':
        consult(arguments)

    if answer == 'p':
        pipeline(arguments)
