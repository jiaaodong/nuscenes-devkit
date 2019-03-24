import rosbag
import numpy as np 
import sys
import os.path as osp 
from data_query import data_query
from tf2_msgs.msg import TFMessage
from geometry_msgs.msg import TransformStamped




if __name__ == "__main__":
    nuscenes_query = data_query(data_root = '/mnt/B4CACF59CACF1690/UbuntuData/data/nuscenes', version = 'v0.1')
    # extract one scene
    scene_token = nuscenes_query.scene[0]['token']
    sample_token_list = nuscenes_query.field2token('sample', 'scene_token', scene_token)
    sample_data_token_list = []
    for sample_token in sample_token_list:
        sample_data_token_list.append(nuscenes_query.field2token('sample_data', 'sample_token', sample_token))
    sample_data_token_list = np.concatenate(sample_data_token_list)
    i = 0
    with rosbag.Bag('output.bag','w') as outbag:
        for sample_data_token in sample_data_token_list:
            sample_data = nuscenes_query.get('sample_data', sample_data_token)
            ego_pose = nuscenes_query.get('ego_pose', sample_data['ego_pose_token'])
            sensor_c = nuscenes_query.get('calibrated_sensor', sample_data['calibrated_sensor_token'])
            sensor_type = nuscenes_query.get('sensor', sensor_c['sensor_token'])
            # write /tf 
            # the ego pose
            tf_car = TransformStamped()
            time_stamp = str(sample_data['timestamp'])
            tf_car.header.stamp.secs = int(time_stamp[:10])
            tf_car.header.stamp.nsecs = int(time_stamp[10:])
            tf_car.header.frame_id = "map"
            tf_car.child_frame_id = "car"
            tf_car.transform.translation.x = ego_pose['translation'][0]
            tf_car.transform.translation.y = ego_pose['translation'][1]
            tf_car.transform.translation.z = ego_pose['translation'][2]
            tf_car.transform.rotation.x = ego_pose['rotation'][1]
            tf_car.transform.rotation.y = ego_pose['rotation'][2]
            tf_car.transform.rotation.z = ego_pose['rotation'][3]
            tf_car.transform.rotation.w = ego_pose['rotation'][0]
            
            tf_sensor = TransformStamped()
            tf_sensor.header.stamp.secs = int(time_stamp[:10])
            tf_sensor.header.stamp.nsecs = int(time_stamp[10:])
            tf_sensor.header.frame_id = "car"
            tf_sensor.child_frame_id = sensor_type['channel']
            tf_sensor.transform.translation.x = sensor_c['translation'][0]
            tf_sensor.transform.translation.y = sensor_c['translation'][1]
            tf_sensor.transform.translation.z = sensor_c['translation'][2]
            tf_sensor.transform.rotation.x = sensor_c['rotation'][1]
            tf_sensor.transform.rotation.y = sensor_c['rotation'][2]
            tf_sensor.transform.rotation.z = sensor_c['rotation'][3]
            tf_sensor.transform.rotation.w = sensor_c['rotation'][0]

            tf_msg = TFMessage([tf_car, tf_sensor])

            outbag.write('/tf', tf_msg, t  = tf_sensor.header.stamp)
            i+=1
            print('finish{}'.format(i))
