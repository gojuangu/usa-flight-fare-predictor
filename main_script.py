import argparse
from d0_gathering import gathering as dga
from d1_transformation import transformation as trn
from d2_model import model as mod
from d3_check import check_flight as chk



def argument_parser():
    parser = argparse.ArgumentParser(description='Set path')
    parser.add_argument('-t', "--path_t", type=str, help='specify database path', required=True)
    parser.add_argument('-c', "--path_c", type=str, help='specify database path', required=True)
    parser.add_argument('-p', "--target_path", type=str, help='specify where store data', required=True)
    parser.add_argument('-p1', "--target_path_1", type=str, help='specify where to store the input data for the model'
                        , required=True)
    parser.add_argument('-1', "--path_1", type=str, help='specify the path where you want to store the parquet files'
                        , required=True)
    parser.add_argument('-2', "--path_2", type=str, help='specify the path where you want to store the parquet files'
                        , required=True)
    args = parser.parse_args()
    return args


def pipeline(arguments):
    print('========================= Pipeline is starting! =========================')
    #dga.unzipp(arguments.path, arguments.target_path)  # unzip files
    #spark = dga.sparkbuilder()
    #dga.spark_parquet(spark, arguments.path_t, arguments.path_c, arguments.target_path)
    #read_parquet_t = dga.read_parquet_ticket(spark, arguments.path_1)
    #read_parquet_c = dga.read_parquet_coupon(spark, arguments.path_2)
    #transform_tickets = trn.tickets_transformation(read_parquet_t)
    #transform_coupons = trn.coupons_transformation(read_parquet_c)
    #trn.join_coupons_tickets(spark, transform_coupons, transform_tickets, arguments.target_path_1)
    mod.data_partition(arguments.target_path_1)
    mod.model(arguments.target_path_1)

    print('========================= Pipeline is complete! =========================')


if __name__ == '__main__':
    arguments = argument_parser()

    print('Welcome to the USA Fare Flight predictor. Please select if you want to start the pipeline os just make a consult:')
    consult = (input('[c]Consult or [p]Pipeline: '))

    if consult == 'c':
        origin = (input('Enter your origin airport code: '))
        dest = (input('Enter your destination airport code: '))
        quarter = (input('Enter your quarter airport code: '))
        fare_class = (input('Enter your fare_class airport code: '))
        print(chk.check_flight(arguments.target_path_1, origin, dest, int(quarter), fare_class))

    if consult == 'p':
        pipeline(arguments)

