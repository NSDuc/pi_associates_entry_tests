from flask import Flask, request
from flask_restful import Resource, Api


if __name__ == "__main__":

    app = Flask(__name__)
    api = Api(app)

    count = 1
    class TradeResource(Resource):
        def get(self):
            global count
            count += 1
            user = request.args.get('user')
            print({'user' : user})
            return {'user' : count}

    # @app.route("/")
    # def index():
    #     return "Congratulations, it's a web app!"

    # @app.route('/data')
    # def data():
    #     # here we want to get the value of user (i.e. ?user=some-value)
    #     user = request.args.get('user')
    #     print(user)
    #     return {'user' : user}

    api.add_resource(TradeResource, '/trade') # Route_1
    app.run(host="127.0.0.1", port=8080, debug=True)
