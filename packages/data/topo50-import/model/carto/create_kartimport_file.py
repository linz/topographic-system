local_pwd = "landinformation"
filekart_carto = r"C:\Data\model\kart_import_carto.txt"
with open(filekart_carto, "w", newline="", encoding="utf-8") as csvfile:
   csvfile.write(f"kart import postgresql://postgres:{local_pwd}@localhost/topo/carto  --primary-key topo_id nz_topo50_carto_text --replace-existing\n") 
   csvfile.write(f"kart import postgresql://postgres:{local_pwd}@localhost/topo/carto  --primary-key topo_id nz_topo50_map_sheet --replace-existing\n") 
