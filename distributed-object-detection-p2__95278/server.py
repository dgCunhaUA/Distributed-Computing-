import argparse
import sys
from flask import Flask, request 
from werkzeug.utils import secure_filename
import requests
import cv2
import json

app = Flask(__name__)

global workers_list
global frames 

workers_list = {}
frames = {}
frame_data = {}

frames_processed =0

def getFrame_data ():
    global frame_data

    return frame_data

@app.route('/', methods=['GET', 'POST'], endpoint='upload_video') 
def upload_video():  

    if request.method == 'POST':  
        print("uploading video")
        file = request.files['video']
        file_name = secure_filename(file.filename)

        #video2image
        vidcap = cv2.VideoCapture(file_name)
        success,image = vidcap.read()
        count = 0
        while success:
            cv2.imwrite("frame%d.jpg" % count, image)     # save frame as JPEG file
            success,image = vidcap.read()
            print ('Read a new frame: ', success)
            
            files = {'file': open('frame' + str(count) + '.jpg', 'rb')}  
            frames[count] = [files, "Waiting"]                  #cria um dicionario com os frames e o seu estado
            count += 1

        frame_count = 0
        frame_count = 0
        ret_x = 0
        ret = True
        
        while (ret):
            for x in workers_list.keys():                     #percorre todos os workers no dicionario
                if ( workers_list[x] == "Disponivel"): 
                    workers_list[x] = "Ocupado"                          #verifica se estao disponiveis
                    url= "http://localhost:" + str(x) + "/frames"

                    for key in frames.keys():                               #percorre os frames para quais estao waiting
                        if ( frames[key][1] == "Waiting"):
                            files = frames[key][0]        # ------
                            frames[key][1] = "Working"
                            try: 
                                r = requests.post(url, files=files, timeout=0.001) #---
                                print(r.text)
                            except:
                                print("")
                            frame_count +=1
                            break  #quebrar o ciclo para passar ao proximo worker disponivel

            ret = False
            for frm in frames.keys():          
                if (frames[frm][1] == "Waiting" or frames[frm][1] == "Working"):
                    ret_x += 1
                    ret = True
        
        getFrame_data()
        obj_detected = []
        obj_person_total = 0
        all_timespent = 0
        max_persons= 24
        persons_frame= {}

        for frame in frame_data:            #para adicionar objetos à lista e contar as pessoas
            frame_obj_person = 0
            for obj in frame_data[frame][0]:
                if ( obj not in obj_detected ):
                    obj_detected.append(obj)
                if (obj == "person" ):
                    obj_person_total += 1
                    frame_obj_person += 1
            if frame_obj_person > max_persons:
                persons_frame[frame] = frame_obj_person             #frames com mais pessoas do que o permitido

            all_timespent += frame_data[frame][1]

        #para fazer o top 3
        count_obj = {}
        for obj in obj_detected:
            if obj in count_obj:
                count_obj[obj] += 1
            else:
                count_obj[obj] = 1

        most_comon_objs = sorted(count_obj, key = count_obj.get, reverse = True)
        top_3_objects = most_comon_objs[:3]


        #para ver os frames com mais pessoas do que o permitido
        for frame in persons_frame:
            print(frame, persons_frame[frame])

        print("Processed frames: ", frame_count)
        print("Average processing time per frame: ", all_timespent/frame_count)
        print("Person objects detected: ", obj_person_total)  
        print("Total classes detected: ", len(obj_detected)) 
        print("Top 3 objects detected: ", top_3_objects)

        return "Finished"


@app.route('/frame_state', methods=['GET', 'POST'])
def get_frame_state():

    if request.method == 'POST':
        get_info = request.get_json()                    #recebe dict com info do frame              {frame0.jpg : [car, person...] }

        frame_data = getFrame_data()                #getFrame_data vai buscar o dict com a informação recebido dos frames                   
        for x in get_info:
            frame_data[x] = get_info[x]                     #juntar a informaçao recebida com a informaçao geral

        for key in frames.keys():                               
            for key2 in get_info:  
                f_name = frames[key][0]["file"].name
                if ( f_name == key2 ):                       #verificar onde esta o frame recebido no dict de frames
                    frames[key][1] = "Done"                              #mudar o estado do frame 
                    print(f_name, "- Done")

        return "Frame State Updated\n"

    


@app.route('/workers', methods=['POST'])
def get_workers():
    
    data = request.get_json()
    for x in data:
        workers_list[x] = data[x]

    return "Worker State Received.\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", help="maximum number of persons in a frame", default=10)
    args = parser.parse_args()

    
    app.run()