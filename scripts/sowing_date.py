import sys
import subprocess
import importlib

def install_package(package_name, import_name=None):
    """
    Simple package installer
    
    Usage:
    install_package("matplotlib")
    install_package("scikit-learn", "sklearn")
    """
    if import_name is None:
        import_name = package_name
    
    try:
        importlib.import_module(import_name)
        print(f"? '{import_name}' already installed")
    except ImportError:
        print(f"Installing '{package_name}'...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
        print(f"? '{package_name}' installed successfully")

# Usage examples
install_package("optim_management")

import os
import glob
import shutil

def optimize(cultivar_folder, cultivar_name):
    """
    Fonction d'optimisation à appeler sur chaque dossier de cultivar
    """
    print(f"?? Optimisation en cours pour {cultivar_name}...")
    print(f"   Dossier: {cultivar_folder}")
    
    # Exemple d'actions d'optimisation :
    # 1. Lister les fichiers NetCDF
    nc_files = glob.glob(os.path.join(cultivar_folder, "*.nc"))
    print(f"   {len(nc_files)} fichiers NetCDF trouvés")
    
    # 2. Traitement spécifique (à adapter)
    for nc_file in nc_files:
        # Votre logique d'optimisation ici
        file_size = os.path.getsize(nc_file) / (1024 * 1024)  # Taille en MB
        print(f"   ?? {os.path.basename(nc_file)}: {file_size:.2f} MB")
        
        # Exemple: compression, recalculation, etc.
        # optimize_netcdf(nc_file)
    
    # 3. Retourner un résultat (optionnel)
    return len(nc_files)

def main():
    infolder = "/input"
    outfolder = "/output"
    
    infiles = glob.glob(os.path.join(infolder, "**", "*.nc"), recursive=True)
    
    # Extraire les cultivars
    cultivars = list(set([
        os.path.basename(f).split("_")[2] 
        for f in infiles 
        if len(os.path.basename(f).split("_")) > 2
    ]))
    
    print(f"Cultivars : {cultivars}")
    
    for culti in cultivars:
        culti_files = [f for f in infiles if os.path.basename(f).split("_")[2] == culti]
        print(f"\n?? Traitement de {culti}: {len(culti_files)} fichiers")
        
        # Créer le sous-dossier de sortie
        outfolder_culti = os.path.join(outfolder, culti)
        os.makedirs(outfolder_culti, exist_ok=True)
        
        # Copier les fichiers
        for source_file in culti_files:
            new_filename = os.path.basename(source_file).replace("_Mgt1", "").replace("2.nc", "2.0.nc")
            dest_file = os.path.join(outfolder_culti, new_filename)
            shutil.copy2(source_file, dest_file)
        
        print(f"? Fichiers copiés dans: {outfolder_culti}")
        
        # ?? APPEL DE LA FONCTION OPTIMIZE sur le dossier
        result = optimize(outfolder_culti, culti)
        print(f"? Optimisation terminée pour {culti} - {result} fichiers traités")

if __name__ == "__main__":
    main()
