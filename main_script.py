import argparse
from d0_gathering import gathering as dga
from d1_transformation import transformation as trn


def argument_parser():
    parser = argparse.ArgumentParser(description='Set path')
    parser.add_argument('-t', "--path_t", type=str, help='specify database path', required=True)
    parser.add_argument('-c', "--path_c", type=str, help='specify database path', required=True)
    parser.add_argument('-p', "--target_path", type=str, help='specify where store data', required=True)
    parser.add_argument('-1', "--path_1", type=str, help='specify database path', required=True)
    parser.add_argument('-2', "--path_2", type=str, help='specify database path', required=True)
    args = parser.parse_args()
    return args


def main(arguments):
    print('Starting the pipeline...')
   # dga.unzipp(arguments.path, arguments.target_path)  # unzip files
    spark = dga.sparkbuilder()
    #dga.spark_parquet(spark, arguments.path_t, arguments.path_c, arguments.target_path)
    read_parquet_t = dga.read_parquet_ticket(spark, arguments.path_1)
    read_parquet_c = dga.read_parquet_coupon(spark, arguments.path_2)
    transform_tickets = trn.tickets_transformation(read_parquet_t)
    transform_coupons = trn.coupons_transformation(read_parquet_c)
    trn.join_coupons_tickets(spark, transform_coupons, transform_tickets)
    print('========================= Pipeline is complete! =========================')


if __name__ == '__main__':
    # print('Welcome to the USA Fare Flight predictor. Please')
    # origin = (input('Enter your origin airport code: '))
    # destination = (input('Enter your origin airport code: '))
    # title = f'Average plane ticket for a flight from {origin} to {destination} is:'
    arguments = argument_parser()
    main(arguments)