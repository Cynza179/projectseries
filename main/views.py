from django.shortcuts import render
from django.http import JsonResponse
from django.contrib.auth.decorators import login_required
import pandas as pd
import io
import base64

def leer_archivo_con_codificacion(file):
    codificaciones = ['utf-8', 'windows-1252', 'latin-1']

    for codificacion in codificaciones:
        try:
            contenido = file.read().decode(codificacion)
            print(f"Archivo leído con codificación: {codificacion}")
            return contenido
        except UnicodeDecodeError:
            print(f"Error al leer con codificación {codificacion}. Intentando con la siguiente...")

    raise ValueError("Ninguna de las codificaciones fue exitosa.")

@login_required
def index(request):
    if request.method == 'POST' and request.FILES.get('file'):
        file = request.FILES['file']
        
        try:
            # Leer el archivo usando diferentes codificaciones
            contenido = leer_archivo_con_codificacion(file)
            df = pd.read_csv(io.StringIO(contenido), sep="|", on_bad_lines='skip')
            
            # Procesar el DataFrame como en tu script original
            nombres_columnas = ['Periodo', 'CUO', 'Corre_CUO','F_Emision','F_Vcmto','Tipo_CdP','Serie_CdP','Corr_CdP','Consolidado','Tipo_Doc','Num_Doc','Denom_Social','Valor_Export',
                                'Base_Imp','Base_Dscto','IGV_Base','IGV_Dscto','Base_Exon','Base_Inaf','ISC','Base_Arroz','IGV_Arroz','Impto_CBP','Otros_Concp','Total_CdP','Moneda',
                                'Tipo_Camb','F_Emis_CdP_Mod','Tipo_CdP_Mod','Serie_CdP_Mod','Corr_CdP_Mod','Ident_Oper','Incons_T_Camb','Ind_Cancel','Oport_Anot','Libre_Util']
            
            df.columns = nombres_columnas
            df = df[df['Tipo_CdP'] != 0]
            df = df.sort_values(by=["Tipo_CdP", "Serie_CdP", "Corr_CdP"])
            
            df["previous_Corr_CdP"] = df["Corr_CdP"].shift(1)
            df["diff"] = df["Corr_CdP"].diff()
            missing_correlatives = df[df["diff"] != 1]
            
            df['group'] = missing_correlatives.groupby(["Tipo_CdP", "Serie_CdP"]).ngroup()
            df = df[df.groupby('group').Corr_CdP.rank(method='first') > 1]
            df = df.drop(columns=['group'])[["Tipo_CdP","Serie_CdP","Corr_CdP"]]
            
            # Guardar el resultado en un archivo Excel en memoria
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Sheet1')
            output.seek(0)
            
            # Codificar el archivo Excel en base64
            excel_data = base64.b64encode(output.getvalue()).decode()
            
            return JsonResponse({
                'success': True,
                'file': excel_data,
                'filename': 'missing_correlatives.xlsx'
            })
        
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    
    return render(request, 'main/index.html')