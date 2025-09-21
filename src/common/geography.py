# src/common/geography.py

import math
import pandas as pd

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calcula a distância em km entre dois pontos geográficos."""
    if any(v is None or pd.isna(v) for v in [lat1, lon1, lat2, lon2]):
        return float('inf')  # Retorna infinito se alguma coordenada for nula

    R = 6371  # Raio da Terra em km
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = (math.sin(dLat / 2) * math.sin(dLat / 2) +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dLon / 2) * math.sin(dLon / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance