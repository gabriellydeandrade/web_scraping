from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from datetime import datetime
import concurrent.futures
import threading
import json
import os
import logging
from tqdm import tqdm

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Função para acessar o portal com os parametros desejados
def consultar(navegador):
    url = 'https://transparencia.mprj.mp.br/web/novo-portal-transparencia/processos-distribuidos-novo'
    navegador.get(url)

def get_element(driver, element_id):
    return WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, element_id)))

def select_option(select_element, value):
    Select(select_element).select_by_value(value)

def collect_data(driver, comarca_name, orgao_name, tipo_doc_name, membro_name):
    col_comarca, col_orgao, col_tipo_doc, col_membro = [], [], [], []
    col_numero, col_distribuido_em, col_submetido_em, col_tipo, col_unidade = [], [], [], [], []

    while True:
        linhas_tabela = driver.find_elements(By.CLASS_NAME, 'tr-modal')
        for linha in linhas_tabela:
            colunas = linha.find_elements(By.TAG_NAME, 'td')
            if not colunas:
                continue

            colunas = list(colunas)
            col_numero.append(colunas[0].text)
            col_distribuido_em.append(colunas[1].text)
            col_submetido_em.append(colunas[2].text)
            col_tipo.append(colunas[3].text)
            col_unidade.append(colunas[4].text)
            col_comarca.append(comarca_name)
            col_orgao.append(orgao_name)
            col_tipo_doc.append(tipo_doc_name)
            col_membro.append(membro_name)

        try:
            next_page = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, './/following-sibling::li/a')))
            next_page.click()
        except:
            break

    return col_comarca, col_orgao, col_tipo_doc, col_membro, col_numero, col_distribuido_em, col_submetido_em, col_tipo, col_unidade

def save_to_csv(name, col_comarca, col_orgao, col_tipo_doc, col_membro, col_numero, col_distribuido_em, col_submetido_em, col_tipo, col_unidade):
    name = name.replace('/', '-')
    file_name = f'arquivos/{name}_{datetime.now().strftime("%d-%m-%Y_%H-%M-%S")}.csv'
    scraping_values = {
        'Comarca': col_comarca,
        'Orgão de execução': col_orgao,
        'Tipo de documento': col_tipo_doc,
        'Membro': col_membro,
        'Número': col_numero,
        'Distribuído em': col_distribuido_em,
        'Submetido em': col_submetido_em,
        'Tipo': col_tipo,
        'Unidade': col_unidade
    }
    df = pd.DataFrame.from_dict(scraping_values)
    df.to_csv(file_name, index=False)

def save_progress(comarcas_visitadas, filename='progress.json'):
    with open(filename, 'w') as f:
        json.dump(comarcas_visitadas, f)

def load_progress(filename='progress.json'):
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    return []

def process_comarca(comarca, comarcas_visitadas_lock, comarcas_visitadas, progress_bar):
    comarca_value = comarca.get_attribute('value')
    if not comarca_value or comarca.get_attribute('innerHTML') in comarcas_visitadas:
        progress_bar.update(1)
        return

    driver = webdriver.Chrome()
    consultar(driver)

    select_option(get_element(driver, comarca_id), comarca_value)
    comarca_name = comarca.text

    orgaos = get_element(driver, orgao_id)
    for orgao in orgaos.find_elements(By.TAG_NAME, "option"):
        orgao_value = orgao.get_attribute('value')
        if not orgao_value:
            continue

        select_option(orgaos, orgao_value)
        orgao_name = orgao.text

        tipos = get_element(driver, tipo_documento_id)
        for tipo in tipos.find_elements(By.TAG_NAME, "option"):
            tipo_value = tipo.get_attribute('value')
            if not tipo_value:
                continue

            select_option(tipos, tipo_value)
            tipo_doc_name = tipo.text

            membros = get_element(driver, membro_id)
            for membro in membros.find_elements(By.TAG_NAME, "option"):
                membro_value = membro.get_attribute('value')
                if not membro_value:
                    continue

                select_option(membros, membro_value)
                membro_name = membro.text

                get_element(driver, btn_buscar_id).click()

                col_comarca, col_orgao, col_tipo_doc, col_membro, col_numero, col_distribuido_em, col_submetido_em, col_tipo, col_unidade = collect_data(driver, comarca_name, orgao_name, tipo_doc_name, membro_name)

                driver.find_element(By.CSS_SELECTOR, 'button.close.btn.btn-unstyled').click()

    with comarcas_visitadas_lock:
        save_to_csv(comarca_name, col_comarca, col_orgao, col_tipo_doc, col_membro, col_numero, col_distribuido_em, col_submetido_em, col_tipo, col_unidade)
        comarcas_visitadas.append(comarca.get_attribute('innerHTML'))
        save_progress(comarcas_visitadas)

    driver.quit()
    progress_bar.update(1)

def main():
    driver = webdriver.Chrome()
    consultar(driver)
    comarcas_visitadas = load_progress()

    comarcas = get_element(driver, comarca_id).find_elements(By.TAG_NAME, "option")

    comarcas_visitadas_lock = threading.Lock()

    with tqdm(total=len(comarcas), desc="Processando Comarcas") as progress_bar:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_comarca, comarca, comarcas_visitadas_lock, comarcas_visitadas, progress_bar) for comarca in comarcas]
            concurrent.futures.wait(futures)

    driver.quit()

if __name__ == "__main__":
    comarca_id = 'comarcaId'
    orgao_id = 'orgaoId'
    tipo_documento_id = 'documentoId'
    membro_id = 'membroId'
    btn_buscar_id = 'export'
    linha_tabela_class = 'tr-modal'
    pagination_css = 'li.page-item.active'

    main()
