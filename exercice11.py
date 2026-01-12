"""
EXERCICE 11 : Climatologie urbaine - Profils saisonniers
Objectif : Calculer les profils mensuels pour chaque ville
"""

import json
from datetime import datetime
from collections import defaultdict
import statistics
from kafka import KafkaProducer

class SeasonalProfiler:
    def __init__(self):
        self.profiles = defaultdict(lambda: defaultdict(list))
    
    def add_monthly_data(self, city, country, month, data_points):
        """Ajoute des données pour un mois spécifique"""
        city_key = f"{city}_{country}"
        
        for data in data_points:
            self.profiles[city_key][month].append({
                'temperature': data.get('temperature'),
                'windspeed': data.get('windspeed'),
                'has_alert': data.get('has_alert', False),
                'date': data.get('date')
            })
    
    def calculate_profile(self, city, country):
        """Calcule le profil saisonnier pour une ville"""
        city_key = f"{city}_{country}"
        monthly_profile = {}
        
        for month in range(1, 13):
            month_data = self.profiles[city_key].get(month, [])
            
            if month_data:
                temps = [d['temperature'] for d in month_data if d['temperature'] is not None]
                winds = [d['windspeed'] for d in month_data if d['windspeed'] is not None]
                alerts = [d['has_alert'] for d in month_data]
                
                monthly_profile[month] = {
                    'avg_temperature': statistics.mean(temps) if temps else None,
                    'avg_windspeed': statistics.mean(winds) if winds else None,
                    'alert_probability': sum(alerts) / len(alerts) if alerts else 0,
                    'data_points': len(month_data),
                    'month_name': self._get_month_name(month)
                }
        
        return monthly_profile
    
    def _get_month_name(self, month):
        months = {
            1: 'Janvier', 2: 'Février', 3: 'Mars', 4: 'Avril',
            5: 'Mai', 6: 'Juin', 7: 'Juillet', 8: 'Août',
            9: 'Septembre', 10: 'Octobre', 11: 'Novembre', 12: 'Décembre'
        }
        return months.get(month, f'Mois {month}')

def exercice11():
    print("=" * 80)
    print("EXERCICE 11 : CLIMATOLOGIE URBAINE - PROFILS SAISONNIERS")
    print("=" * 80)
    
    profiler = SeasonalProfiler()
    
    # Données simulées pour Paris (3 années de données mensuelles)
    for year in [2022, 2023, 2024]:
        for month in range(1, 13):
            # Générer des données réalistes
            base_temp = 10 + month * 1.5  # Variation saisonnière
            base_wind = 5 + abs(month - 6) * 0.5  # Plus de vent en hiver/été
            
            month_data = []
            for day in range(1, 29):  # 28 jours par mois simplifié
                date = f"{year}-{month:02d}-{day:02d}"
                
                # Ajouter de la variabilité
                temp = base_temp + (day % 10) - 5
                wind = base_wind + (day % 7) - 3
                has_alert = (temp > 30) or (wind > 15)  # Alerte si conditions extrêmes
                
                month_data.append({
                    'temperature': temp,
                    'windspeed': wind,
                    'has_alert': has_alert,
                    'date': date
                })
            
            profiler.add_monthly_data("Paris", "France", month, month_data)
    
    # Calculer le profil
    profile = profiler.calculate_profile("Paris", "France")
    
    print("📅 PROFIL SAISONNIER - PARIS (moyennes sur 3 ans)")
    print("-" * 70)
    print(f"{'Mois':<12} {'Température':<15} {'Vent':<12} {'Risque alerte':<15}")
    print("-" * 70)
    
    for month in range(1, 13):
        data = profile.get(month, {})
        if data:
            temp = data['avg_temperature']
            wind = data['avg_windspeed']
            alert = data['alert_probability'] * 100  # Pourcentage
            
            print(f"{data['month_name']:<12} {temp:>6.1f}°C{'':<6} {wind:>5.1f} m/s{'':<4} {alert:>5.1f}%")
    
    print("-" * 70)
    
    # Sauvegarde dans "HDFS" simulé (fichier JSON)
    import os
    os.makedirs("hdfs-data/France/Paris/seasonal_profile/", exist_ok=True)
    
    profile_file = "hdfs-data/France/Paris/seasonal_profile/profile.json"
    with open(profile_file, 'w') as f:
        json.dump({
            'city': 'Paris',
            'country': 'France',
            'profile': profile,
            'calculated_at': datetime.now().isoformat(),
            'data_years': '2022-2024'
        }, f, indent=2)
    
    print(f"✅ Profil sauvegardé dans: {profile_file}")
    
    # Publication Kafka
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    summary_message = {
        'city': 'Paris',
        'country': 'France',
        'profile_summary': {
            'hottest_month': max(profile.items(), key=lambda x: x[1]['avg_temperature'])[0],
            'windiest_month': max(profile.items(), key=lambda x: x[1]['avg_windspeed'])[0],
            'highest_alert_risk': max(profile.items(), key=lambda x: x[1]['alert_probability'])[0]
        },
        'timestamp': datetime.now().isoformat()
    }
    
    producer.send('seasonal_profiles', summary_message)
    producer.flush()
    
    print("✅ Résumé publié dans Kafka (topic: seasonal_profiles)")
    
    return profile

if __name__ == "__main__":
    exercice11()