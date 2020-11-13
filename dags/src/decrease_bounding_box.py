from pyntcloud import PyntCloud
import os

def reduce_bounding_box(ds, artifact=None, base_data_path=None, **kwargs):
    qrcode = artifact.split('_')[1]
    pcd_path = base_data_path + '/pcd/' + qrcode + '/100'
    output_path = base_data_path + '/pdc-reduced-bb/' + qrcode + '/100'
    
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