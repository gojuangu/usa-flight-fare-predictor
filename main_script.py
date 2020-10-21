import argparse
from d_gathering import gathering as dga
from d_transformation import transformation as trn


def argument_parser():
    parser = argparse.ArgumentParser(description='Set path')
    parser.add_argument('-d', "--path", type=str, help='specify database path', required=True)
    parser.add_argument('-t', "--target_path", type=str, help='specify where store data', required=True)
    args = parser.parse_args()
    return args


def main(arguments):
    print('Starting the pipeline...')
   # dga.unzipp(arguments.path, arguments.target_path)  # unzip files
    spark = dga.sparkbuilder()
    parquet = dga.spark_parquet(spark, arguments.target_path)
    read_parquet = dga.read_parquet(parquet.target_path_pt, parquet.targer_path_pc)
    transform_tickets = trn.tickets_transformation(read_parquet.df_t)
    transform_coupons = trn.coupons_transformation(read_parquet.df_c)
    trn.join_coupons_tickets(transform_coupons, transform_tickets)
    print('========================= Pipeline is complete! =========================')


if __name__ == '__main__':
    # origin = (input('Enter your origin airport code: '))
    # destination = (input('Enter your origin airport code: '))
    # title = f'Average plane ticket for a flight from {origin} to {destination} is:'
    arguments = argument_parser()
    main(arguments)