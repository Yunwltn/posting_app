
class Config :
    HOST = 'yh-db.ccyrbljtknuy.ap-northeast-2.rds.amazonaws.com'
    DATABASE = 'posting_db'
    DB_USER = 'posting_user'
    DB_PASSWORD = 'yh1234db'

    SALT = 'dskj29jcdn12jn'

    # JWT 관련 변수를 셋팅
    JWT_SECRET_KEY = 'yhacdemy20230105##hello'
    JWT_ACCESS_TOKEN_EXPIRES = False # 만료없이 설정
    PROPAGATE_EXCEPTIONS = True # 에러가 나면 보여줄것

    
    # AWS 관련 키
    ACCESS_KEY = 'AKIAYG44LDKLA5XQ27GD'
    SECRET_ACCESS = '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'
    
    # S3 버킷
    S3_BUCKET = 'yunwltn-yh-test'
    # S3 Location
    S3_LOCATION = 'https://yunwltn-yh-test.s3.ap-northeast-2.amazonaws.com/'

    # 네이버 API키
    NAVER_CLIENT_ID = 'QitiIvuk4DoOqIHP6nkr'
    NAVER_CLIENT_SECRET = 'p1jRN9GX3J'