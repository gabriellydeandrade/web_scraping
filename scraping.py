from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from datetime import datetime

timeout = 5

def consultar(navegador):
    url = 'https://transparencia.mprj.mp.br/web/novo-portal-transparencia/processos-distribuidos-novo'
    navegador.get(url)

def get_element(driver, element_id):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.ID, element_id)))

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
            next_page = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((By.XPATH, './/following-sibling::li/a'))) # TODO: verificar se isso aqui funciona sem pegar  pagination_css = 'li.page-item.active'
                                                                                                                                   # actual_page = driver.find_element(By.CSS_SELECTOR, pagination_css)   
            next_page.click()
        except:
            break

    return col_comarca, col_orgao, col_tipo_doc, col_membro, col_numero, col_distribuido_em, col_submetido_em, col_tipo, col_unidade

def save_to_csv(name, col_comarca, col_orgao, col_tipo_doc, col_membro, col_numero, col_distribuido_em, col_submetido_em, col_tipo, col_unidade):
    name = name.replace('/', '-')
    file_name = f'{name}_{datetime.now().strftime("%d-%m-%Y_%H-%M-%S")}'
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
    df.to_csv(f'arquivos/{file_name}.csv', index=False)

def main():
    driver = webdriver.Chrome()
    consultar(driver)
    comarcas_visitados = []

    comarca_id = 'comarcaId'
    orgao_id = 'orgaoId'
    tipo_documento_id = 'documentoId'
    membro_id = 'membroId'
    btn_buscar_id = 'export'

    comarcas = get_element(driver, comarca_id)
    for comarca in comarcas.find_elements(By.TAG_NAME, "option"):
        if comarca.get_attribute('innerHTML') in comarcas_visitados:
            continue

        comarca_value = comarca.get_attribute('value')
        if not comarca_value:
            continue

        select_option(comarcas, comarca_value)
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

        save_to_csv(comarca_name, col_comarca, col_orgao, col_tipo_doc, col_membro, col_numero, col_distribuido_em, col_submetido_em, col_tipo, col_unidade)

        comarcas_visitados.append(comarca.get_attribute('innerHTML'))

    driver.quit()

if __name__ == "__main__":
    main()
