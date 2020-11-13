from pyntcloud import PyntCloud
import pandas as pd
import os
from src.Task3 import adjust_axis

def reduce_bounding_box(ds, artifact=None, base_data_path=None, **kwargs):
    qrcode = artifact.split('_')[1]
    pcd_path = base_data_path + '/pcd/' + qrcode + '/100'
    output_path = base_data_path + '/pcd-reduced-bb/' + qrcode + '/100'
    
    point_cloud = PyntCloud.from_file(pcd_path + '/' + artifact)
    
    to_drop = point_cloud.points[(point_cloud.points.x > point_cloud.points.x.quantile(q=0.95))].index
    point_cloud.points.drop(index=to_drop, inplace=True)
    to_drop = point_cloud.points[(point_cloud.points.y > point_cloud.points.y.quantile(q=0.85)) | (point_cloud.points.y < point_cloud.points.y.quantile(q=0.15))].index
    point_cloud.points = point_cloud.points.drop(index=to_drop)
    to_drop = point_cloud.points[(point_cloud.points.z > point_cloud.points.z.quantile(q=0.95))].index
    point_cloud.points = point_cloud.points.drop(index=to_drop)
    point_cloud.xyz = point_cloud.points[['x', 'y', 'z']].to_numpy()
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        
    point_cloud.to_file(output_path + '/' + artifact[:-4] + '.xyz')
    
def rotate_pcd(ds, artifact=None, base_data_path=None, **kwargs):
    qrcode = artifact.split('_')[1]
    pcd_path = base_data_path + '/pcd-reduced-bb/' + qrcode + '/100'
    output_path = base_data_path + '/pcd-reduced-bb-rotated/' + qrcode + '/100'
    
    angles = pd.read_csv(base_data_path + '/pcd-angles-from-maskrnn-hist.csv')
    slope = angles[angles.artifact == artifact].slope
    
    adjuster = adjust_axis.maskrnn_hist(base_data_path + '/calibration-lenovo-phab.txt')
    adjuster._m = slope
    
    point_cloud = PyntCloud.from_file(pcd_path + '/' + artifact[:-4] + '.xyz')
    
    adjusted = adjuster.transform_pyntcloud(point_cloud)
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    
    adjusted.to_file(output_path + '/' + artifact[:-4] + '.xyz')
    
    