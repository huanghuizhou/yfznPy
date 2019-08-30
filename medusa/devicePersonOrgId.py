import logging
import sys
import pymysql

#修正device库 t_device_person org_id 字段


# DB_HOST = '192.168.1.172'
# DB_USER = 'root'
# DB_PASSWD = 'Uni@ubi123'

# DB_HOST = 'localhost'
# DB_USER = 'root'
# DB_PASSWD = '123456'

DB_HOST=sys.argv[1]
DB_USER=sys.argv[2]
DB_PASSWD=sys.argv[3]


DB_PORT = 3306
DB_SOURCE_DB = 'uniubi_medusa_organization'
DB_TARGET_DB = 'uniubi_medusa_device'

yfzn_source_db = pymysql.connect(host=DB_HOST,
                                          user=DB_USER,
                                          passwd=DB_PASSWD,
                                          db=DB_SOURCE_DB,
                                          port=DB_PORT,  # 3306
                                          use_unicode=True,
                                          charset="utf8",
                                        cursorclass=pymysql.cursors.DictCursor)

yfzn_target_db = pymysql.connect(host=DB_HOST,
                                          user=DB_USER,
                                          passwd=DB_PASSWD,
                                          db=DB_TARGET_DB,
                                          port=DB_PORT,  # 3306
                                          use_unicode=True,
                                          charset="utf8",
                                        cursorclass=pymysql.cursors.DictCursor)

def get_logger(name):
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)

    # Standard output handler
    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(logging.Formatter('%(levelname)s - %(name)s:%(lineno)s: %(message)s'))
    log.addHandler(sh)
    return log


logger = get_logger(__file__)

#设置orgId
def doGetDevicePerson():
    device_cursor = yfzn_target_db.cursor()
    deviceSql='select person_guid as personGuid,person_id as personId from t_device_person'
    device_cursor.execute(deviceSql)
    results = device_cursor.fetchall()

    logger.debug('start add total : %s' % (str(len(results))))

    upNum=0
    for devicePerson in results:
        personId=devicePerson['personId']
        personGuid=devicePerson['personGuid']

        try:
            orgId=doGetOrgId(personId)
            if(orgId==None):
                continue
            deviceUpdateSql='''update t_device_person set org_id= %s where person_guid = %s and person_id= %s'''
            device_cursor.execute(deviceUpdateSql,(orgId,personGuid,personId))
            up_results = device_cursor.fetchall()

            logger.debug('%s add success personId:%s personGuid:%s' % (str(upNum),personId,personGuid))

            upNum=upNum+1
            # 每50条 提交一次,
            if(upNum%50==0):
                logger.debug('db commit %s ' % (str(upNum)))

                yfzn_target_db.commit()

        except Exception as e:
            logger.error('%s add fail personId:%s personGuid:%s' % (str(upNum),personId,personGuid))

    #最终提交
    yfzn_target_db.commit()


#获取企业id
def doGetOrgId(personId):
    cursor = yfzn_source_db.cursor()
    sql = '''select t.organization_id as orgId from t_employee t where t.id = %s'''
    cursor.execute(sql,(personId))
    org_results = cursor.fetchall()
    if(len(org_results)!=1):
        return
    return org_results[0]['orgId']

def main():
    doGetDevicePerson()


if __name__ == '__main__':
    main()
