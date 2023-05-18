from pyspark.sql import SparkSession
import numpy as np
import pandas as pd

class ForceDirectedGraph:
    def __init__(self, df_edges):
        spark = SparkSession.builder.appName('ForceDirectedGraph').getOrCreate()
        self.edges = spark.createDataFrame(df_edges)

        nodes = self.edges.select('source').union(self.edges.select('target')).distinct()
        random_positions = np.random.rand(nodes.count(), 2)
        nodes_with_positions = nodes.rdd.zipWithIndex().map(lambda x: (x[0][0], random_positions[x[1]]))
        self.node_positions = spark.createDataFrame(nodes_with_positions, ["node", "position"])

    def calculate_forces(self, node, data, repulsive_force_constant, attractive_force_constant):
        source_edges = data[0]
        target_edges = data[1]
        position = data[2]
        displacement = np.array([0, 0])

        # calculate repulsive forces
        for other_node, other_position in self.node_positions.rdd.collect():
            if node != other_node:
                distance = np.linalg.norm(position - other_position)
                if distance != 0:
                    force = repulsive_force_constant / distance
                    displacement += (position - other_position) / distance * force

        # calculate attractive forces
        for edge in source_edges:
            other_position = self.node_positions.filter(self.node_positions.node == edge).collect()[0][1]
            distance = np.linalg.norm(position - other_position)
            if distance != 0:
                force = attractive_force_constant * distance
                displacement -= (position - other_position) / distance * force

        for edge in target_edges:
            other_position = self.node_positions.filter(self.node_positions.node == edge).collect()[0][1]
            distance = np.linalg.norm(position - other_position)
            if distance != 0:
                force = attractive_force_constant * distance
                displacement += (position - other_position) / distance * force

        return node, displacement

    def layout(self, repulsive_force_constant=0.5, attractive_force_constant=0.1, maximum_displacement=0.1, iterations=100):
        for _ in range(iterations):
            sources = self.edges.groupBy('source').agg(F.collect_list('target').alias('target'))
            targets = self.edges.groupBy('target').agg(F.collect_list('source').alias('source'))

            node_data = self.node_positions.join(sources, self.node_positions.node == sources.source, how='left')\
                .join(targets, self.node_positions.node == targets.target, how='left')\
                .rdd.map(lambda x: (x[0], (x[2], x[3], x[1])))

            displacements = node_data.map(lambda x: self.calculate_forces(x[0], x[1], repulsive_force_constant, attractive_force_constant))

            self.node_positions = displacements.map(lambda x: (x[0], np.minimum(np.maximum(x[1], -maximum_displacement), maximum_displacement)))\
                .toDF(["node", "position"])

        return pd.DataFrame(self.node_positions.rdd.map(lambda x: (x[0], list(x[1]))).collect(), columns=['vertex', 'position'])
