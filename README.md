---

# 📊 Data Mart Regulatorio + Segmentación Comercial (SMV, SBS, BVL)

Pipeline end-to-end de inteligencia regulatoria para el sector financiero peruano. Integra datos de los 3 principales reguladores (SMV, SBS, BVL) para construir un Data Mart unificado y segmentar empresas supervisadas por perfil de riesgo y potencial comercial.

---

## ⚙️ Componente 1 : Automatizacion Ent Reg Assurance (ZIP) + Proyecto Ent Reg Assurance Dev (ZIP)— Extractor de Entidades Regulatorias

### ¿Qué hace?
Descarga y procesa automáticamente los dictámenes financieros en PDF de **202 empresas supervisadas por la SMV**, extrayendo campos clave mediante un pipeline combinado de Python + Power Automate + AI Builder.

### Flujo del pipeline
```
SMV Portal → Selenium scraper → PDF descargado → pdfplumber / AI Builder (GPT-4.1 mini)
→ Extracción: Auditor | Honorarios | Tipo de Opinión | Directorio
→ Output: Excel consolidado en SharePoint / Microsoft Fabric
```

### Fuentes de datos integradas
| Regulador | Entidades | Variables extraídas |
|---|---|---|
| SMV | 202 empresas supervisadas | Auditor, honorarios, tipo de opinión, directorio |
| SBS | Bancarias, financieras, cajas, AFPs, seguros | Razón social, RUC, tipo, representante legal |
| BVL | 268 empresas, 9 sectores | Sector, datos bursátiles, código ISIN |

### Stack técnico
```python
# Librerías principales
selenium          # Navegación y descarga automatizada del portal SMV
pdfplumber        # Extracción de texto de dictámenes PDF
pandas            # Transformación y limpieza de datos
openai / AI Builder (GPT-4.1 mini)  # Extracción inteligente de campos
power automate    # Orquestación del flujo end-to-end
microsoft fabric  # Almacenamiento y consulta del Data Mart
```

---

## 🤖 Componente 2 — Segmentación Comercial con K-Means Proyecto Segmentación-Análisis Dev (ZIP)

### ¿Qué hace?
Consolida los datos de los 3 reguladores en un **Data Mart unificado** y aplica clustering K-Means para segmentar las +200 empresas supervisadas por perfil de riesgo y potencial comercial, habilitando la priorización de prospectos del equipo de ventas.

### Flujo del análisis
```
Data Mart unificado (SMV + SBS + BVL)
→ Limpieza y normalización de variables
→ Selección de features: tamaño, sector, auditor, tipo de opinión, etc.
→ Método del codo + índice de silueta → número óptimo de clusters
→ K-Means clustering
→ Output: Segmentación por perfil de riesgo y potencial comercial
```

### Variables del modelo
| Variable | Fuente | Descripción |
|---|---|---|
| Tipo de auditor | SMV | Big4 vs. firma local |
| Tipo de opinión | SMV | Limpia, con salvedades, adversa |
| Honorarios de auditoría | SMV | Proxy de tamaño de empresa |
| Sector | BVL | Clasificación sectorial |
| Tipo de entidad | SBS | Banco, financiera, AFP, etc. |

### Clusters resultantes (ejemplo)
```
Cluster 0 — Alto riesgo / baja prioridad:    opinión con salvedades, auditor local
Cluster 1 — Perfil consolidado / alta prio.: opinión limpia, Big4, honorarios altos
Cluster 2 — Perfil emergente / media prio.:  opinión limpia, firma local, sector creciente
```

---

## 🚀 Cómo ejecutar

```bash
# 1. Clonar el repositorio
git clone https://github.com/alvarovillakoba/data-mart-regulatorio.git
cd data-mart-regulatorio

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Ejecutar extractor SMV
python 01_extractor/smv_scraper.py

# 4. Consolidar Data Mart
python 02_segmentacion/data_mart_builder.py

# 5. Ejecutar segmentación
python 02_segmentacion/clustering.py
```

---

## 📦 Requirements

```
pandas
numpy
selenium
pdfplumber
scikit-learn
matplotlib
seaborn
openpyxl
requests
```

---

## 📌 Resultados

- ✅ 202 empresas SMV procesadas con extracción automatizada de dictámenes
- ✅ 268 empresas BVL integradas en 9 sectores
- ✅ Data Mart unificado SMV + SBS + BVL operativo en Microsoft Fabric
- ✅ Segmentación K-Means en 3 clusters habilitando priorización comercial
- ✅ Reducción de proceso manual de semanas a minutos

---

## 👤 Autor

**Álvaro Villanueva Kobayashi**
Business Process Engineer @ BCP | Data Science & Analytics
[LinkedIn](https://linkedin.com/in/tu-perfil) · [GitHub](https://github.com/alvarovillakoba)

---

¿Lo ajustamos en algo? Por ejemplo puedo cambiar los clusters resultantes si tienes los nombres reales, o agregar capturas/badges de tecnologías.
