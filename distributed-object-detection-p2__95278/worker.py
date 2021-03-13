import argparse
import os
from flask import Flask, request
from werkzeug.utils import secure_filename
import requests
import json
import sys
import numpy as np
import core.utils as utils
import tensorflow as tf
from core.yolov3 import YOLOv3, decode
from core.config import cfg
from PIL import Image
import cv2
import time


app = Flask(__name__)
#Read class names
class_names = {}
with open(cfg.YOLO.CLASSES, 'r') as data:
    for ID, name in enumerate(data):
        class_names[ID] = name.strip('\n')

# Setup tensorflow, keras and YOLOv3
input_size   = 416
input_layer  = tf.keras.layers.Input([input_size, input_size, 3])
feature_maps = YOLOv3(input_layer)

bbox_tensors = []
for i, fm in enumerate(feature_maps):
    bbox_tensor = decode(fm, i)
    bbox_tensors.append(bbox_tensor)

model = tf.keras.Model(input_layer, bbox_tensors)
utils.load_weights(model, "./yolov3.weights")


def worker_state(server_address, server_port):
    url= "http://localhost:5000/workers"
    payload = {args.worker_port: "Disponivel"}
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    print("Sending Worker State...")
    print(response.text)
    pass


@app.route('/frames', methods=['GET', 'POST'])
def recv_frames():
    if request.method == 'POST':
        frame = request.files['file']
        frame.save(secure_filename(frame.filename))
        print("frame recebido: ", frame, "\n")
           

        begin = time.time()
        original_image      = cv2.imread(frame.filename) #you can and should replace this line to receive the image directly (not from a file)

        original_image      = cv2.cvtColor(original_image, cv2.COLOR_BGR2RGB)
        original_image_size = original_image.shape[:2]

        image_data = utils.image_preporcess(np.copy(original_image), [input_size, input_size])
        image_data = image_data[np.newaxis, ...].astype(np.float32)

        pred_bbox = model.predict(image_data)
        pred_bbox = [tf.reshape(x, (-1, tf.shape(x)[-1])) for x in pred_bbox]
        pred_bbox = tf.concat(pred_bbox, axis=0)

        bboxes = utils.postprocess_boxes(pred_bbox, original_image_size, input_size, 0.3)
        bboxes = utils.nms(bboxes, 0.45, method='nms')

        # We have our objects detected and boxed, lets move the class name into a list
        objects_detected = []
        for x0,y0,x1,y1,prob,class_id in bboxes:
            objects_detected.append(class_names[class_id])

        end = time.time()
        print(f"Objects Detected: {objects_detected}")
        time_spent = end - begin
        send_frame_info(objects_detected, time_spent, frame)   

        return frame.filename + " Treated\n"

    return 'Frame tratado\n'


def send_frame_info(objects_detected, time_spent, frame):
    url= "http://localhost:5000/frame_state"
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    payload = {frame.filename: [ objects_detected, time_spent ]}
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    print("Sending Frame State...")
    print(response.text)

    worker_state(args.server_address, args.server_port)                     #mudar estado de ocupado para disponivel


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-address", help="server address", default="localhost")
    parser.add_argument("--server-port", help="server address port", default=3456)
    parser.add_argument("--worker-port", help="worker address port", default=5001)

    args = parser.parse_args()
    

    worker_state(args.server_address, args.server_port)
    app.run(port=args.worker_port)