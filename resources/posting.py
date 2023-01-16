from flask import request
from flask_jwt_extended import get_jwt_identity, jwt_required
from flask_restful import Resource
from mysql_connection import get_connection
from mysql.connector import Error
from datetime import datetime
import boto3
from config import Config

class PostingResource(Resource) :
    # 포스팅 작성 API
    @jwt_required()
    def post(slef) :

        userId = get_jwt_identity()
        
        # from=data
        # photo : file
        # content : text

        # 둘중 하나라도 없으면 안됨 세이프 코딩
        if 'photo' not in request.files or 'content' not in request.form :
            return {'error' : '데이터를 정확히 보내세요'}, 400
        
        file = request.files['photo']
        content = request.form['content']

        # 이미지 데이터만 받게 세이프 코딩
        if 'image' not in file.content_type :
            return {'error' : '이미지 파일이 아닙니다.'}

        # 사진명을 유니크하게 변경해서 S3에 업로드
        # aws 콘솔로 가서 IAM 유저만들고 S3 버킷을 만들어서 config.py에 입력

        # 파일명 유니크하게 변경
        current_time = datetime.now()
        new_file_name = str(userId) + current_time.isoformat().replace(':', '_') + '.' + file.content_type.split('/')[-1]
        file.filename = new_file_name

        # 파일 S3에 업로드 (boto3사용)
        client =  boto3.client('s3', aws_access_key_id= Config.ACCESS_KEY, aws_secret_access_key= Config.SECRET_ACCESS)
        try :
            client.upload_fileobj(file, Config.S3_BUCKET, new_file_name, ExtraArgs= {'ACL' : 'public-read', 'ContentType' : file.content_type})
        
        except Exception as e :
            return {"error" : str(e)}, 500

        # 저장된 사진의 imgUrl
        imgUrl = Config.S3_LOCATION + new_file_name

        # S3저장된 사진을 Amazon Rekognition 사용하여 Object Detection
        client = boto3.client('rekognition', 'ap-northeast-2', aws_access_key_id= Config.ACCESS_KEY, aws_secret_access_key= Config.SECRET_ACCESS)
        response = client.detect_labels(Image= {'S3Object' : {'Bucket' : Config.S3_BUCKET, 'Name' : new_file_name}}, MaxLabels= 5)

        # 태그 저장 
        tag_list = []
        for row in response['Labels'] :
            tag_list.append(row['Name'])

        # DB에 저장
        try :
            connection = get_connection()

            # 포스팅 테이블 저장
            query = '''insert into posting
                    (userId, imgUrl, content)
                    values (%s, %s, %s);'''

            record = (userId, imgUrl, content)

            cursor = connection.cursor()

            cursor.execute(query, record)

            # 저장한 포스팅 테이블 id 가져오기
            postingId = cursor.lastrowid

            # 태그네임 테이블 저장
            # tag_list가 tag_name 테이블에 들어있는지 확인해서 있으면
            # 그 tag_name의 아이디를 가져오고 없으면 tag_name에 넣어준다
            for name in tag_list :
                query = '''select *
                        from tag_name
                        where name = %s ;'''
                record = (name, )
                cursor = connection.cursor(dictionary= True)
                cursor.execute(query, record)
                result_list = cursor.fetchall()

                if len(result_list) == 0 :
                    query = '''insert into tag_name
                    (name)
                    values (%s);'''
                    record = (name, )
                    cursor = connection.cursor()
                    cursor.execute(query, record)
                    tagId = cursor.lastrowid

                else :
                    tagId = result_list[0]['id']

                # tag 테이블에 postingId와 tagId를 저장한다
                query = '''insert into tag
                        (postingId, tagId)
                        values
                        (%s, %s)'''

                record = (postingId, tagId)

                cursor = connection.cursor()

                cursor.execute(query, record)

            # 커밋은 쿼리문이 끝난 마지막에 해준다
            connection.commit()
        
            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()

            return {"error" : str(e)}, 500

        return {"result" : "success"}, 200

    # 전체 포스팅 리스트 가져오는 API
    def get(slef) :
        offset = request.args.get('offset')
        limit = request.args.get('limit')

        try :
            connection = get_connection()

            query = '''select *
                    from posting
                    limit ''' + offset + ''' , ''' + limit + ''' ; '''
                    
            cursor = connection.cursor(dictionary= True)

            cursor.execute(query, )

            result_list = cursor.fetchall()

            i = 0
            for row in result_list :
                result_list[i]['createdAt'] = row['createdAt'].isoformat()
                result_list[i]['updatedAt'] = row['updatedAt'].isoformat()
                i = i + 1

            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()

            return {"error" : str(e)}, 500

        return {"result" : "success", "items" : result_list, "count" : len(result_list)}, 200

class MyPostingResource(Resource) :
    # 내 포스팅 리스트만 가져오는 API
    @jwt_required()
    def get(slef) :
        userId = get_jwt_identity()
        offset = request.args.get('offset')
        limit = request.args.get('limit')

        try :
            connection = get_connection()

            query = '''select *
                    from posting
                    where userId = %s
                    limit ''' + offset + ''' , ''' + limit + ''' ; '''

            record = (userId, )

            cursor = connection.cursor(dictionary= True)

            cursor.execute(query, record)

            result_list = cursor.fetchall()

            i = 0
            for row in result_list :
                result_list[i]['createdAt'] = row['createdAt'].isoformat()
                result_list[i]['updatedAt'] = row['updatedAt'].isoformat()
                i = i + 1

            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()

            return {"error" : str(e)}, 500

        return {"result" : "success", "items" : result_list, "count" : len(result_list)}, 200

class ModifyPostingResource(Resource) :
    # 포스팅 수정 API
    @jwt_required()
    def post(slef, postingId) :

        userId = get_jwt_identity()
        
        # from=data
        # photo : file
        # content : text

        # 둘중 하나라도 없으면 안됨 세이프 코딩
        if 'photo' not in request.files or 'content' not in request.form :
            return {'error' : '데이터를 정확히 보내세요'}, 400
        
        file = request.files['photo']
        content = request.form['content']

        # 이미지 데이터만 받게 세이프 코딩
        if 'image' not in file.content_type :
            return {'error' : '이미지 파일이 아닙니다.'}

        # 사진명을 유니크하게 변경해서 S3에 업로드
        current_time = datetime.now()
        new_file_name = current_time.isoformat().replace(':', '_') + '.' + file.content_type.split('/')[-1]
        file.filename = new_file_name
        client =  boto3.client('s3', aws_access_key_id= Config.ACCESS_KEY, aws_secret_access_key= Config.SECRET_ACCESS)

        try :
            client.upload_fileobj(file, Config.S3_BUCKET, new_file_name, ExtraArgs= {'ACL' : 'public-read', 'ContentType' : file.content_type})
        
        except Exception as e :
            return {"error" : str(e)}, 500

        # 저장된 사진의 imgUrl
        imgUrl = Config.S3_LOCATION + new_file_name

        # DB에 저장되어있는 포스팅정보 수정
        try :
            connection = get_connection()

            query = '''update posting
                    set
                    imgUrl = %s,
                    content = %s
                    where id = %s and userId = %s;'''

            record = (imgUrl, content, postingId, userId)

            cursor = connection.cursor()

            cursor.execute(query, record)

            connection.commit()

            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()

            return {"error" : str(e)}, 500

        return {"result" : "success"}, 200

    # 포스팅 삭제 API
    @jwt_required()
    def delete(slef, postingId) :

        userId = get_jwt_identity()

        try :
            connection = get_connection()

            query = ''' delete from posting
                    where id = %s and userId = %s ; '''
            
            record = (postingId, userId)

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

class followeePostingResource(Resource) :
    # 친구들 포스팅 리스트만 가져오는 API
    @jwt_required()
    def get(slef) :
        userId = get_jwt_identity()
        offset = request.args.get('offset')
        limit = request.args.get('limit')

        try :
            connection = get_connection()

            query = '''select p.id, p.imgUrl, p.content, u.id, u.email, p.updatedAt,
                    ifnull(count(postingId), 0) as likeCnt,
                    if(l.userId is null, 0, 1) as 'favorite'
                    from follow f
                    left join posting p on f.followeeId = p.userId
                    left join user u on f.followeeId = u.id
                    left join posting_db.like l on p.id = l.postingId
                    where followerId = %s
                    group by p.id
                    limit ''' + offset + ''' , ''' + limit + ''' ; '''

            record = (userId, )

            cursor = connection.cursor(dictionary= True)

            cursor.execute(query, record)

            result_list = cursor.fetchall()

            i = 0
            for row in result_list :
                result_list[i]['updatedAt'] = row['updatedAt'].isoformat()
                i = i + 1

            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()

            return {"error" : str(e)}, 500

        return {"result" : "success", "items" : result_list, "count" : len(result_list)}, 200

