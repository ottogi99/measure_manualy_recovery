import datetime
import glob
import json
import os

import pandas as pd
import pyodbc

'''
장흥댐 CR1000 데이터로거 수동 복구 프로그램
2022.04.06. ~ 
'''


def load_config(json_path):
    if not os.path.exists(json_path):
        logger.error(f'{json_path} 파일이 존재하지 않습니다.')
        exit(-1)

    with open(json_path, 'r', encoding='UTF-8') as fp:
        json_obj = json.load(fp)

    return json_obj


def read_measure_data(csv_path):
    global df_sensors, config

    if not os.path.exists(csv_path):
        logger.error(f'{csv_path} 파일이 존재하지 않습니다.')
        exit(-1)

    df_measure = pd.read_csv(csv_path, header=None, skiprows=4)  # 설정한 해더로 DATA 파일을 읽어들인다.
    csv_filename = os.path.basename(csv_path)

    data_file_list = config['logger']['data_files']
    logger_obj_list = config['logger']['items']

    if csv_filename not in data_file_list:
        logger.info(f'It is not appropriate data file.({os.path.basename(csv_filename)})')
        exit(-1)

    for idx, logger_obj in enumerate(logger_obj_list):
        if csv_filename == logger_obj['data_path']:
            pos1 = f'logger{idx+1}_pos1'
            pos2 = f'logger{idx+1}_pos2'
            break
        else:
            continue

    condition = (df_sensors[pos1] != 0)
    df_position = df_sensors.loc[condition, [pos1, pos2]].astype('int64')

    for idx, row in df_position.iterrows():
        m01 = row[pos1]
        m02 = row[pos2]
        df_sensors.TIMESTAMP = df_measure.iloc[0][0]
        df_sensors.M01 = df_measure.iloc[0][m01]
        df_sensors.M02 = df_measure.iloc[0][m02] if m02 != 0 else 999999


def calculate_measure_data(json_path):
    global df_sensors

    if not os.path.exists(json_path):
        logger.error(f'{json_path} 파일이 존재하지 않습니다.')
        exit(-1)

    with open(json_path, 'r', encoding='UTF-8') as fp:
        json_obj = json.load(fp)

    for index, series in df_sensors.iterrows():
        sensor_type = series['type']

        F01 = series['gf01']
        F02 = series['gf02']
        F03 = series['gf03']
        F04 = series['gf04']
        F05 = series['gf05']
        F06 = series['gf06']
        I01 = series['init01']
        I02 = series['init02']
        M01 = series['m01']
        M02 = series['m02']
        C01 = series['c01']
        C02 = series['c02']
        C03 = series['c03']
        C04 = series['c04']
        PC01 = series['pc01']
        PC02 = series['pc02']

        formulas = json_obj['sensorInfo'][sensor_type]['formula']
        # if series['type'] == 'JM':
        #     logger.debug(f'M01: {M01}, M02: {M02}')
        #     logger.debug(f'I01: {I01}, I02: {I02}')
        #     logger.debug(f'F01: {F01}, F02: {F02}, F03: {F03}, F04: {F04}, F05: {F05}')
        BC01 = C01
        BC02 = C02
        # logger.debug(f'C01: {C01}, C02: {C02}')
        # logger.debug(df_sensors.iloc[index])
        for i, formula in enumerate(formulas):
            if i == 0:
                C01 = round(eval(formula), 3)
                series['c01'], series['pc01'] = C01, C01
            elif i == 1:
                C02 = round(eval(formula), 3)
                series['c02'], series['pc02'] = C02, C02
            elif i == 2:
                C03 = round(eval(formula), 3)
                series['c03'] = C03
            elif i == 3:
                C04 = round(eval(formula), 3)
                series['c04'] = C04

        df_sensors.iloc[index] = series
        # logger.debug(df_sensors.iloc[index])

        # -3.49e-07과 -0.00000034943값을 통해 계산한 값에 오차가 발생함.(즉, 엑셀결과값과 소수점 오차 발생)
        # r = (8395.76 * -0.09071) + (8395.76 ** 2 * -0.00000034943) + 852.63
        # r2 = (8395.76 * -0.09071) + (8395.76 ** 2 * -3.49e-07) + 852.63

        # logger.debug(f"NAME: {series['name']}) C01: {C01}, C02: {C02}, C03: {C03}, C04: {C04}")
        # if series['type'] == 'JM':
        #     logger.debug(f"NAME: {series['name']}) {C01} - {BC01} = {round(C01-BC01, 4)}")


def save_to_db():
    global config, df_sensors

    connection = None
    inserted_count = 0

    for db in config['db']:
        provider = 'DRIVER={{Tibero 6 ODBC Driver}};SERVER={0};PORT={1};UID={2};PWD={3};DATABASE={4}'.format(
            db['server'],
            db['port'],
            db['uid'],
            db['pwd'],
            db['database'],
        )

        # try:
        #     connection = pyodbc.connect(provider)
        # except pyodbc.InterfaceError as ex:
        #     # error_state = ex.args[0]
        #     logger.error('[DB 연결 오류] {0}'.format(ex))
        #     exit(-1)

        for idx, sensor in df_sensors.iterrows():
            query = "INSERT INTO DULLNDGGDT(OBSDT, DAMCD, SENID, MEAVAL1, MEAVAL2, CALVAL1, CALVAL2, CALVAL3, CALVAL4) " \
                    "VALUES ('{0}', '{1}', '{2}', {3}, {4}, {5}, {6}, {7})".format(
                sensor['date'],
                config['dam']['code'],
                sensor['senid'],
                sensor['m01'],
                sensor['m02'],
                sensor['c01'],
                sensor['c02'],
                sensor['c03'],
                sensor['c04'],
            )

            logger.info(f'[QUERY] {query}')
            # try:
            #     logger.info(f'[QUERY] {query}')
            #     cursor = connection.cursor()
            #     cursor.execute(query)
            #     connection.commit()
            #     inserted_count += 1
            #
            # except pyodbc.Error as ex:
            #     # sqlstate = ex.args[0]
            #     logger.error('[pyodbc Exception] sqlstate:{0}'.format(ex))
            #     continue

    logger.info(f'총 {inserted_count} 건의 데이터가 입력되었습니다.')

    if connection is not None:
        connection.close()


def save_to_csv(csv_path):
    global df_sensors

    if os.path.exists(csv_path):
        now = datetime.datetime.now()
        try:
            backup_path = f'./csv/sensors_{now.strftime("%Y%m%d_%H%M%S")}.csv'
            os.rename(csv_path, backup_path)
            logger.info(f'기존 파일 ({csv_path})을 {backup_path}로 변경합니다.')
        except Exception as e:
            logger.error(e)

    df_sensors.to_csv(csv_path, sep=',', na_rep='NAN')


if __name__ == '__main__':
    import logging

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler('app.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info('--- 장흥댐 CR1000 수동복구 프로그램 시작 ---')

    config_path = './json/config.json'
    config = load_config(config_path)
    if config is None:
        logger.info(f'Unable to load configuration file: {os.path.basename(config_path)}')
        exit(-1)

    # df_sensors = pd.read_csv('./csv/sensors.csv', index_col='id')
    # df_sensors = pd.read_csv('./csv/out_merged.csv', index_col='id')
    # print(df_sensors)
    # print(df_sensors.sort_index(ascending=True))
    # df_mapping = pd.read_csv('./csv/mapping.csv', index_col='id')
    # df_mapping = pd.read_csv('./csv/mapping2.csv', index_col='id')
    # print(df_mapping.sort_index(ascending=True))
    # merged = df_sensors.join(df_mapping)
    # r = df_sensors.merge(df_mapping, how='outer', left_on='id')

    # r = pd.concat([df_sensors, df_mapping], ignore_index=True, join='outer', keys=['id'])

    # for index, row in merged.iterrows():
    #     print(f"{index}, {row['n1']}, {row['name']}")

    # merged.to_csv('./csv/out_merged.csv', sep=',', encoding='utf-8')
    # merged.to_csv('./csv/out_merged2.csv', sep=',', encoding='utf-8')
    # exit(0)

    # 1) .dat 파일로 부터 측정값 추출 및 sensors 데이터프레임에 저장
    sensor_info_path = './csv/sensors.csv'
    df_sensors = pd.read_csv(sensor_info_path).fillna(0)

    filter_path = './data/*.dat'
    dat_files = glob.glob(filter_path)
    for dat_file in dat_files:
        read_measure_data(dat_file)

    # logger.debug(df_sensors[['m01', 'm02']])

    # 2) 계산식(json)을 통해 측정한 값을 연산하여 sensors 데이터프레임에 저장
    formula_info_path = 'json/formula.json'
    calculate_measure_data(formula_info_path)

    # 3) 갱신된 데이터프레임을 파일로 저장한다. (이전 산출값을 사용하는 경우가 있으므로, DB를 사용하지 않는다.)
    save_to_csv(sensor_info_path)

    # 4) sensor 데이터프레임의 정보를 DB로 저장
    save_to_db()

    logger.info('--- 장흥댐 CR1000 수동복구 프로그램 종료 ---')
