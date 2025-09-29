import http.client
import time
import asyncio
import random
from sortedcontainers import SortedDict
import docker
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
import threading
import json
import math
import threading
import copy
import os
import mysql.connector
import Datastructures


global global_schema
num_retries = 3
client = docker.from_env()
global metadata_obj
global servers_obj
shard_id_object_mapping = {}
