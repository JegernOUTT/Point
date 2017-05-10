import random
import pickle

import math
import numpy as np

from builtins import type

from matplotlib import pyplot as plt
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

from sklearn.cluster import KMeans


class ClusterByPolygon:
    filename = 'doublegis_api/data/cluster_centers.pickle'
    latitude_degree_meters = 111131.745
    longitude_degree_meters = 78846.80572069259

    def __init__(self,
                 region,
                 radius_meters=200,
                 filling_points_count=200000,
                 multiply_factor=0.6,
                 mode='file'):
        self.radius_meters = radius_meters
        self.lat_radius_degree = radius_meters / ClusterByPolygon.latitude_degree_meters
        self.lon_radius_degree = radius_meters / ClusterByPolygon.longitude_degree_meters
        self.filling_points_count = filling_points_count
        self.multiply_factor = multiply_factor

        self.polygon = Polygon(region.bounds)

        if mode == 'file':
            self.cluster_centers = self.load_clusters()['clusters']
        elif mode == 'kmeans':
            self.cluster_centers = self.calculate_kmeans_clusters()
            self.save_clusters()
        elif mode == 'random':
            self.cluster_centers = self.calculate_random_clusters()
            self.save_clusters()

    def __get_random_point_in_polygon(self):
        (minx, miny, maxx, maxy) = self.polygon.bounds
        while True:
            p = Point(random.uniform(minx, maxx), random.uniform(miny, maxy))
            if self.polygon.contains(p):
                return p

    def calculate_kmeans_clusters(self):
        random_points = [self.__get_random_point_in_polygon().xy
                         for _ in range(self.filling_points_count)]
        random_points = np.array([[point[0][0], point[1][0]] for point in random_points])

        ellips_area_degree = math.pi * self.lat_radius_degree * self.lon_radius_degree
        polygon_area = self.polygon.area
        clusters_n = round(polygon_area // ellips_area_degree)
        clusters_n += int(clusters_n * self.multiply_factor)

        clusters = KMeans(n_clusters=clusters_n,
                          max_iter=20,
                          n_init=3,
                          verbose=1,
                          # n_jobs=-1,
                          init='random').fit_predict(random_points)
        return [[np.mean(random_points[clusters == i][:, 0]),
                 np.mean(random_points[clusters == i][:, 1])]
                for i in range(clusters_n)]

    def calculate_random_clusters(self):
        random_points = [self.__get_random_point_in_polygon().xy
                         for _ in range(self.filling_points_count)]
        return np.array([[point[0][0], point[1][0]] for point in random_points])

    def plot_circles(self):
        x, y = self.polygon.exterior.xy
        plt.plot(x, y, color='#6699cc', alpha=0.7, linewidth=3, solid_capstyle='round')
        circles = [plt.Circle((center[0], center[1]), self.lat_radius_degree,
                              color='r', alpha=.2, linewidth=1)
                   for center in self.cluster_centers]
        ax = plt.gca()
        for circle in circles:
            ax.add_artist(circle)
        plt.show()

    def plot_with_filled_circles(self, filled_circles):
        x, y = self.polygon.exterior.xy
        plt.plot(x, y, color='#6699cc', alpha=0.7, linewidth=3, solid_capstyle='round')

        not_found = [[coords[0], coords[1]] for coords, count in filled_circles.items() if count == -1]
        found = [[coords[0], coords[1]] for coords, count in filled_circles.items() if count >= 0]

        not_found_circles = [plt.Circle((center[0], center[1]), self.lat_radius_degree,
                                        color='r', alpha=.2, linewidth=2)
                             for center in not_found]
        found_circles = [plt.Circle((center[0], center[1]), self.lat_radius_degree,
                                    color='g', alpha=.2, linewidth=2)
                         for center in found]
        ax = plt.gca()
        for circle in found_circles:
            ax.add_artist(circle)
        for circle in not_found_circles:
            ax.add_artist(circle)
        plt.show()

    def plot_dots_on_polygon(self, dots, metros):
        x, y = self.polygon.exterior.xy
        plt.plot(x, y, color='#6699cc', alpha=0.7, linewidth=3, solid_capstyle='round')

        circles_dots = [plt.Circle((center[0], center[1]), self.lat_radius_degree * 10,
                              color='g', alpha=.2, linewidth=2)
                   for center in dots]
        circles_metros = [plt.Circle((center[0], center[1]), self.lat_radius_degree * 50,
                              color='r', alpha=.2, linewidth=4)
                   for center in metros]
        ax = plt.gca()
        for circle in circles_dots:
            ax.add_artist(circle)
        for circle in circles_metros:
            ax.add_artist(circle)
        plt.show()

    def plot_new_data(self, data, metros):
        x, y = self.polygon.exterior.xy
        plt.plot(x, y, color='#6699cc', alpha=0.7, linewidth=3, solid_capstyle='round')

        new_buildings = [plt.Circle((b.longitude, b.latitude), self.lat_radius_degree * 10,
                                    color='k', alpha=.2, linewidth=2)
                         for b in data['new_buildings']]
        removed_buildings = [plt.Circle((b.longitude, b.latitude), self.lat_radius_degree * 10,
                                        color='b', alpha=.2, linewidth=2)
                             for b in data['not_found_buildings']]

        new_filials = [plt.Circle((f.longitude, f.latitude), self.lat_radius_degree * 10,
                                    color='g', alpha=.2, linewidth=2)
                       for f in data['new_filials']]
        removed_filials = [plt.Circle((b.longitude, b.latitude), self.lat_radius_degree * 10,
                                        color='r', alpha=.2, linewidth=2)
                           for b in data['not_found_filials']]

        circles_metros = [plt.Circle((center[0], center[1]), self.lat_radius_degree * 50,
                              color='k', alpha=.1, linewidth=6)
                          for center in metros]
        ax = plt.gca()
        for c in new_buildings:
            ax.add_artist(c)
        for c in removed_buildings:
            ax.add_artist(c)
        for c in new_filials:
            ax.add_artist(c)
        for c in removed_filials:
            ax.add_artist(c)
        for c in circles_metros:
            ax.add_artist(c)
        plt.show()

    def plot_mul_dots_on_polygon(self, dots1, dots2, metros):
        x, y = self.polygon.exterior.xy
        plt.plot(x, y, color='#6699cc', alpha=0.7, linewidth=3, solid_capstyle='round')

        circles_dots1 = [plt.Circle((center[0], center[1]), self.lat_radius_degree * 10,
                              color='g', alpha=.2, linewidth=2)
                         for center in dots1]
        circles_dots2 = [plt.Circle((center[0], center[1]), self.lat_radius_degree * 10,
                              color='b', alpha=.1, linewidth=3)
                         for center in dots2]
        circles_metros = [plt.Circle((center[0], center[1]), self.lat_radius_degree * 50,
                              color='r', alpha=.1, linewidth=4)
                          for center in metros]
        ax = plt.gca()
        for circle in circles_dots1:
            ax.add_artist(circle)
        for circle in circles_dots2:
            ax.add_artist(circle)
        for circle in circles_metros:
            ax.add_artist(circle)
        plt.show()

    def save_clusters(self):
        with open(ClusterByPolygon.filename, 'wb') as f:
            pickle.dump({'lat_radius': self.lat_radius_degree,
                         'lon_radius': self.lon_radius_degree,
                         'clusters': self.cluster_centers}, f)

    def load_clusters(self):
        with open(ClusterByPolygon.filename, 'rb') as f:
            data = pickle.load(f)
            assert type(data) == dict
            assert 'lat_radius' in data.keys()
            assert 'lon_radius' in data.keys()
            assert 'clusters' in data.keys()
            return data

