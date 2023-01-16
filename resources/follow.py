from flask import request
from flask_jwt_extended import get_jwt_identity, jwt_required
from flask_restful import Resource
from mysql_connection import get_connection
from mysql.connector import Error

class FollowResource(Resource) :
    @jwt_required()
    def post(self, followeeId) : 
    
        user_id = get_jwt_identity()

        try :
            connection = get_connection()

            query = '''insert into follow(followerId, followeeId)
                    values(%s, %s);'''

            record = ( user_id, followeeId )

            cursor = connection.cursor()

            cursor.execute(query, record)

            connection.commit()

            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()

            return {"result" : "fail", "error" : str(e)}, 500

        return {"result" : "success"}, 200

    @jwt_required()
    def delete(self, followeeId) :

        userId = get_jwt_identity()

        try :
            connection = get_connection()

            query = ''' delete from follow
                    where followerId = %s and followeeId = %s ; '''
            
            record = (userId, followeeId)

            cursor = connection.cursor()

            cursor.execute(query, record)

            connection.commit()

            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()
            return {"result" : "fail", "error" : str(e)}, 500

        return {"result" : "success"}, 200