from flask import Flask
from flask_jwt_extended import JWTManager
from flask_restful import Api
from config import Config
from resources.follow import FollowResource
from resources.like import likeResource
from resources.posting import ModifyPostingResource, MyPostingResource, PostingResource, followeePostingResource
from resources.user import UserLoginResource, UserRegisterResource, UserLogoutResource
from resources.user import jwt_blacklist

app = Flask(__name__)

app.config.from_object(Config)

jwt = JWTManager(app)

@jwt.token_in_blocklist_loader
def check_if_token_is_revoked(jwt_header, jwt_payload) : 
    jti = jwt_payload['jti']
    return jti in jwt_blacklist

api = Api(app)

api.add_resource(UserRegisterResource, '/user/register')
api.add_resource(UserLoginResource, '/user/login')
api.add_resource(UserLogoutResource, '/user/logout')

api.add_resource(PostingResource, '/posting')
api.add_resource(MyPostingResource, '/posting/me')
api.add_resource(ModifyPostingResource, '/posting/<int:postingId>')
api.add_resource(followeePostingResource, '/posting/followee')

api.add_resource(FollowResource, '/follow/<int:followeeId>')
api.add_resource(likeResource, '/like/<int:postingId>')

if __name__ == '__main__' :
    app.run()