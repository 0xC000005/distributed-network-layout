import pandas as pd
import numpy as np

class ForceDirectedGraph:
    def __init__(self, df_edges):
        self.df_edges = df_edges
        self.nodes = pd.concat([df_edges['source'], df_edges['target']]).unique()
        self.node_positions = {node: np.random.rand(2) for node in self.nodes}
        self.displacement = {node: np.array([0, 0]) for node in self.nodes}

    def calculate_distance(self, node_i, node_j):
        position_difference = self.node_positions[node_i] - self.node_positions[node_j]
        return np.sqrt(np.sum(np.square(position_difference)))

    def calculate_repulsive_forces(self, repulsive_force_constant):
        for i in range(len(self.nodes)):
            for j in range(i + 1, len(self.nodes)):
                node_i = self.nodes[i]
                node_j = self.nodes[j]
                distance = self.calculate_distance(node_i, node_j)
                if distance != 0:
                    force = repulsive_force_constant / distance
                    self.displacement[node_i] += force * (self.node_positions[node_i] - self.node_positions[node_j]) / distance
                    self.displacement[node_j] -= force * (self.node_positions[node_i] - self.node_positions[node_j]) / distance

    def calculate_attractive_forces(self, attractive_force_constant):
        for _, edge in self.df_edges.iterrows():
            node_i = edge['source']
            node_j = edge['target']
            distance = self.calculate_distance(node_i, node_j)
            if distance != 0:
                force = attractive_force_constant * distance
                self.displacement[node_i] -= force * (self.node_positions[node_i] - self.node_positions[node_j]) / distance
                self.displacement[node_j] += force * (self.node_positions[node_i] - self.node_positions[node_j]) / distance

    def update_positions(self, maximum_displacement):
        for node in self.nodes:
            delta = self.displacement[node]
            length = np.sqrt(np.sum(np.square(delta)))
            self.node_positions[node] += (delta / length) * min(length, maximum_displacement)

    def layout(self, repulsive_force_constant=0.5, attractive_force_constant=0.1, maximum_displacement=0.1, iterations=100):
        for _ in range(iterations):
            self.displacement = {node: np.array([0, 0]) for node in self.nodes}
            self.calculate_repulsive_forces(repulsive_force_constant)
            self.calculate_attractive_forces(attractive_force_constant)
            self.update_positions(maximum_displacement)
        return pd.DataFrame(self.node_positions.items(), columns=['vertex', 'position'])
