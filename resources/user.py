from datetime import datetime
from flask import request
from flask_jwt_extended import create_access_token, get_jwt, get_jwt_identity, jwt_required
from flask_restful import Resource
from mysql_connection import get_connection
from mysql.connector import Error
from email_validator import validate_email, EmailNotValidError
from utils import check_password, hash_password

class UserRegisterResource(Resource) :
    def post(self) :
        # {"email": "abcd@naver.com",
        # "password": "bbb1234"}

        data = request.get_json()

        try :
            validate_email( data["email"] )
        except EmailNotValidError as e :
            print(str(e))
            return {'error' : str(e)}, 400

        if len(data["password"]) < 4 or len(data["password"]) > 20 :
            return {'error' : '비밀번호 길이 확인'}, 400

        hashed_password = hash_password(data["password"])
        print(hashed_password)

        try :
            connection = get_connection()
            query = '''insert into user
                    (email, password)
                    values
                    (%s, %s);'''
            
            record = (data["email"], hashed_password)

            cursor = connection.cursor()

            cursor.execute(query, record)

            connection.commit()

            user_id = cursor.lastrowid

            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()
            return {"error" : str(e)}, 500

        access_token = create_access_token(user_id)
        return {"result" : "success", "access_token" : access_token}, 200

class UserLoginResource(Resource) :
    def post(self) :
        # {"email": "abcd@naver.com",
        # "password": "bbb1234"}

        data = request.get_json()

        try :
            connection = get_connection()

            query = '''select *
                    from user
                    where email = %s ;'''

            record = (data["email"], )

            cursor = connection.cursor(dictionary=True)

            cursor.execute(query, record)

            result_list = cursor.fetchall()

            if len(result_list) == 0 :
                return {"error" : "회원가입한 사람이 아닙니다"} , 400

            i = 0
            for row in result_list :
                result_list[i]['createdAt'] = row['createdAt'].isoformat()
                i = i + 1

            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()
            return {"error" : str(e)}, 500


        check = check_password( data['password'], result_list[0]['password'] )

        if check == False :
            return {"error" : "비밀번호가 일치하지 않습니다"} , 400

        access_token = create_access_token( result_list[0]['id'] )

        return {"result" : "success", "access_token" : access_token}, 200

jwt_blacklist = set()

class UserLogoutResource(Resource) :
    @jwt_required()
    def post(self) :
        
        jti = get_jwt()['jti']
        print(jti)

        jwt_blacklist.add(jti)

        return {'result' : 'success'}, 200
