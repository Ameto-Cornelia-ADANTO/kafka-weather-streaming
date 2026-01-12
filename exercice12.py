"""
EXERCICE 12 : Validation et enrichissement des profils saisonniers
Objectif : Vérifier et enrichir les profils avec statistiques avancées
"""

import json
import statistics
from datetime import datetime
import os

class ProfileValidator:
    def __init__(self):
        self.validation_rules = {
            'temperature_range': (-50, 60),  # °C
            'windspeed_range': (0, 60),      # m/s
            'required_months': 12,
            'alert_probability_range': (0, 1)
        }
    
    def validate_profile(self, profile):
        """Valide un profil saisonnier"""
        issues = []
        warnings = []
        
        # Vérifier les 12 mois
        if len(profile) != self.validation_rules['required_months']:
            issues.append(f"Manque des mois: {len(profile)}/12 mois présents")
        
        for month, data in profile.items():
            temp = data.get('avg_temperature')
            wind = data.get('avg_windspeed')
            alert_prob = data.get('alert_probability')
            
            # Vérification température
            if temp is not None:
                if not (self.validation_rules['temperature_range'][0] <= temp <= self.validation_rules['temperature_range'][1]):
                    issues.append(f"Mois {month}: Température {temp}°C hors limites")
            else:
                warnings.append(f"Mois {month}: Température manquante")
            
            # Vérification vent
            if wind is not None:
                if not (self.validation_rules['windspeed_range'][0] <= wind <= self.validation_rules['windspeed_range'][1]):
                    issues.append(f"Mois {month}: Vent {wind} m/s hors limites")
            else:
                warnings.append(f"Mois {month}: Vent manquant")
            
            # Vérification probabilité alerte
            if alert_prob is not None:
                if not (self.validation_rules['alert_probability_range'][0] <= alert_prob <= self.validation_rules['alert_probability_range'][1]):
                    issues.append(f"Mois {month}: Probabilité alerte {alert_prob} invalide")
        
        return {
            'is_valid': len(issues) == 0,
            'issues': issues,
            'warnings': warnings,
            'checked_at': datetime.now().isoformat()
        }
    
    def enrich_profile(self, profile):
        """Enrichit le profil avec des statistiques avancées"""
        enriched = {}
        
        # Extraire toutes les valeurs pour calculs globaux
        all_temps = [data['avg_temperature'] for data in profile.values() if data['avg_temperature'] is not None]
        all_winds = [data['avg_windspeed'] for data in profile.values() if data['avg_windspeed'] is not None]
        
        for month, data in profile.items():
            # Statistiques de dispersion pour le mois
            month_data = data.get('raw_data', [])
            
            if month_data:
                temps = [d['temperature'] for d in month_data]
                winds = [d['windspeed'] for d in month_data]
                
                # Calculs avancés
                enriched[month] = {
                    **data,
                    'temperature_stats': {
                        'std': statistics.stdev(temps) if len(temps) > 1 else 0,
                        'min': min(temps) if temps else None,
                        'max': max(temps) if temps else None,
                        'median': statistics.median(temps) if temps else None,
                        'q25': sorted(temps)[len(temps)//4] if len(temps) >= 4 else None,
                        'q75': sorted(temps)[3*len(temps)//4] if len(temps) >= 4 else None
                    },
                    'windspeed_stats': {
                        'std': statistics.stdev(winds) if len(winds) > 1 else 0,
                        'min': min(winds) if winds else None,
                        'max': max(winds) if winds else None,
                        'median': statistics.median(winds) if winds else None,
                        'q25': sorted(winds)[len(winds)//4] if len(winds) >= 4 else None,
                        'q75': sorted(winds)[3*len(winds)//4] if len(winds) >= 4 else None
                    }
                }
            else:
                # Si pas de données brutes, utiliser les valeurs moyennes
                enriched[month] = {
                    **data,
                    'temperature_stats': {
                        'std': statistics.stdev(all_temps) if len(all_temps) > 1 else 0,
                        'min': min(all_temps) if all_temps else None,
                        'max': max(all_temps) if all_temps else None,
                        'median': statistics.median(all_temps) if all_temps else None
                    },
                    'windspeed_stats': {
                        'std': statistics.stdev(all_winds) if len(all_winds) > 1 else 0,
                        'min': min(all_winds) if all_winds else None,
                        'max': max(all_winds) if all_winds else None,
                        'median': statistics.median(all_winds) if all_winds else None
                    }
                }
        
        # Ajouter des statistiques globales
        enriched['_global_stats'] = {
            'annual_avg_temperature': statistics.mean(all_temps) if all_temps else None,
            'annual_avg_windspeed': statistics.mean(all_winds) if all_winds else None,
            'temperature_variability': statistics.stdev(all_temps) if len(all_temps) > 1 else 0,
            'enriched_at': datetime.now().isoformat()
        }
        
        return enriched

def exercice12():
    print("=" * 80)
    print("EXERCICE 12 : VALIDATION ET ENRICHISSEMENT DES PROFILS")
    print("=" * 80)
    
    # Charger le profil de l'exercice 11
    profile_file = "hdfs-data/France/Paris/seasonal_profile/profile.json"
    
    if not os.path.exists(profile_file):
        print("❌ Profil non trouvé. Exécutez d'abord l'Exercice 11.")
        return
    
    with open(profile_file, 'r') as f:
        data = json.load(f)
    
    profile = data.get('profile', {})
    
    print("🔍 VALIDATION DU PROFIL")
    print("-" * 60)
    
    validator = ProfileValidator()
    validation_result = validator.validate_profile(profile)
    
    if validation_result['is_valid']:
        print("✅ Profil VALIDE")
    else:
        print("❌ Profil INVALIDE")
        for issue in validation_result['issues']:
            print(f"   - {issue}")
    
    for warning in validation_result['warnings']:
        print(f"⚠️  {warning}")
    
    print("\n📈 ENRICHISSEMENT DU PROFIL")
    print("-" * 60)
    
    enriched_profile = validator.enrich_profile(profile)
    
    # Exemple d'affichage pour un mois
    sample_month = 7  # Juillet
    if sample_month in enriched_profile:
        month_data = enriched_profile[sample_month]
        print(f"\n📊 Statistiques détaillées - Juillet:")
        print(f"   Température moyenne: {month_data['avg_temperature']:.1f}°C")
        print(f"   Écart-type: ±{month_data['temperature_stats']['std']:.1f}°C")
        print(f"   Plage: {month_data['temperature_stats']['min']:.1f}°C à {month_data['temperature_stats']['max']:.1f}°C")
        print(f"   Médiane: {month_data['temperature_stats']['median']:.1f}°C")
        print(f"   Quartiles: Q25={month_data['temperature_stats']['q25']:.1f}°C, Q75={month_data['temperature_stats']['q75']:.1f}°C")
    
    # Sauvegarde du profil enrichi
    year = datetime.now().year
    enriched_dir = f"hdfs-data/France/Paris/seasonal_profile_enriched/{year}/"
    os.makedirs(enriched_dir, exist_ok=True)
    
    enriched_file = os.path.join(enriched_dir, "profile.json")
    with open(enriched_file, 'w') as f:
        json.dump({
            'city': 'Paris',
            'country': 'France',
            'original_profile': profile,
            'enriched_profile': enriched_profile,
            'validation_result': validation_result,
            'enriched_at': datetime.now().isoformat()
        }, f, indent=2)
    
    print(f"\n✅ Profil enrichi sauvegardé dans: {enriched_file}")
    
    # Statistiques globales
    if '_global_stats' in enriched_profile:
        global_stats = enriched_profile['_global_stats']
        print(f"\n📈 STATISTIQUES GLOBALES ANNUALES:")
        print(f"   Température moyenne annuelle: {global_stats['annual_avg_temperature']:.1f}°C")
        print(f"   Vent moyen annuel: {global_stats['annual_avg_windspeed']:.1f} m/s")
        print(f"   Variabilité température: {global_stats['temperature_variability']:.1f}°C")
    
    return enriched_profile

if __name__ == "__main__":
    exercice12()