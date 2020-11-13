import numpy as np

from torchvision import transforms
from torchvision.models.detection import maskrcnn_resnet50_fpn

def _convert2Dto3D(intrisics, x, y, z, width, height):
    fx = intrisics[0] * float(width)
    fy = intrisics[1] * float(height)
    cx = intrisics[2] * float(width)
    cy = intrisics[3] * float(height)
    tx = (x - cx) * z / fx
    ty = (y - cy) * z / fy
    output = []
    output.append(tx)
    output.append(ty)
    output.append(z)
    return output

class maskrnn_hist():
	def __init__(self, calibration_file):
		"""
		calibration_file: [str] path to calibration.txt file
		"""
		self.maskrnn = maskrcnn_resnet50_fpn(pretrained=True)
		self.toTensor = transforms.ToTensor()

		_ = self.maskrnn.eval()

		with open(calibration_file, 'r') as f:
			calib = f.readlines()
		fx = float(calib[3].split()[0])
		fy = float(calib[3].split()[1])
		cx = float(calib[3].split()[2])
		cy = float(calib[3].split()[3])
		self.intrinsics = [fx, fy, cx, cy]

	def fit(self, depthmap, rgb_image):
		"""
		depthmap: [numpy array] the depthmap
		rgb_image: [PIL image] the corresponding jpeg
		"""
		tensor_rgb = rgb_image.copy()
		tensor_rgb = tensor_rgb.convert("RGB")
		tensor_rgb = self.toTensor(tensor_rgb)

		maskrnn_output = self.maskrnn([tensor_rgb])
		y1, x1, y2, x2 = maskrnn_output[0]['boxes'][0].detach().numpy()

		Ny, Nx, _ = depthmap.shape

		coeffs = list()
		for ivert in range(int(x1*1.25), int(x2*0.75)):
			start = max(int(y1), 0)
			end = min(int(y2), Ny)
			strip = depthmap[start:end, ivert]
			height_values = np.array([_convert2Dto3D(
					self.intrinsics,
					 h, 0, z,
					 Ny, Nx)[0] for h,z in zip(range(start, end),
							   strip)])
			mask = strip > 0
			strip = strip[mask]
			height_values = height_values[mask]

			hi = np.percentile(strip, q=75)
			lo = np.percentile(strip, q=25)

			mask = strip <= hi
			strip = strip[mask]
			height_values = height_values[mask]

			mask = strip >=lo
			strip = strip[mask]
			height_values = height_values[mask]
			
			m, q = np.polyfit(
				x=height_values,
				y=strip,
				deg=1)
			
			coeffs.append(m)

		self.coeffs = np.array(coeffs).reshape(-1,1)

		hist, bin_edges = np.histogram(coeffs, bins=50)
		index = np.argmax(hist)
		self._m = 0.5*(bin_edges[index] + bin_edges[index+1])

	def transform(self, X):
		"""
		X: [numpy array] array of point cloud data
		"""
		angle = self.angle
		Y = X.copy()

		Y[:,0] = np.cos(angle)*X[:,0] - np.sin(angle)*X[:,2]
		Y[:,2] = np.sin(angle)*X[:,0] + np.cos(angle)*X[:,2]

		return Y

	def fit_transform(self, X, depthmap, rgb_image):
		"""
		X: [numpy array] array of point cloud data
		depthmap: [numpy array] the depthmap
		rgb_image: [PIL image] the corresponding jpeg
		"""
		self.fit(depthmap, rgb_image)
		return self.transform(X)

	def transform_pyntcloud(self, pcd):
		"""
		pcd: [PyntCloud] the PyntCloud object
		"""
		from pyntcloud import PyntCloud
		angle = self.angle
		output = PyntCloud(points=pcd.points.copy(deep=True))
	
		output.points.x = np.cos(angle)*pcd.points.x - np.sin(angle)*pcd.points.z
		output.points.z = np.sin(angle)*pcd.points.x + np.cos(angle)*pcd.points.z

		output.xyz = output.points[['x', 'y', 'z']].to_numpy()
		return output


	def fit_transform_pyntcloud(self, pcd, depthmap, rgb_image):
		"""
		pcd: [PyntCloud] the PyntCloud object
		depthmap: [numpy array] the depthmap
		rgb_image: [PIL image] the corresponding jpeg
		"""
		self.fit(depthmap, rgb_image)
		return self.transform_pyntcloud(pcd)

	@property 
	def angle(self):
		return np.abs(np.arctan(self._m))

