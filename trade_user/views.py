from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .tests import post_action, get_action
import json

class UserView(APIView):
    def __init__(self):
        None

    def post(self, request):
        results = post_action(request)
        response_data = {"result": results}
        return Response(response_data, status=status.HTTP_200_OK)

    def get(self, request):
        # GET 요청 처리
        get_action(request)
        data = {"message": "GET request received"}
        return Response(data, status=status.HTTP_200_OK)