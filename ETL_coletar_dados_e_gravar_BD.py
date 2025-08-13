import datetime
import gc
import pathlib
import urllib.parse
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import bs4 as bs
import ftplib
import gzip
import os
import pandas as pd
import psycopg2
import re
import sys
import time
import requests
import urllib.request
import wget
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import threading

# Lock para print thread-safe
print_lock = Lock()

def thread_safe_print(message):
    """Print thread-safe para evitar sobreposição de mensagens"""
    with print_lock:
        print(message)
        sys.stdout.flush()

def check_diff(url, file_name):
    '''
    Verifica se o arquivo no servidor existe no disco e se ele tem o mesmo
    tamanho no servidor.
    '''
    if not os.path.isfile(file_name):
        return True # ainda nao foi baixado

    try:
        response = requests.head(url, timeout=10)
        new_size = int(response.headers.get('content-length', 0))
        old_size = os.path.getsize(file_name)
        if new_size != old_size:
            os.remove(file_name)
            return True # tamanho diferentes
    except:
        return True # em caso de erro, tenta baixar

    return False # arquivos sao iguais

def download_file_with_progress(url, output_path, file_name, thread_id):
    """
    Baixa um arquivo com indicador de progresso
    """
    file_path = os.path.join(output_path, file_name)
    
    if not check_diff(url, file_path):
        thread_safe_print(f"[Thread {thread_id}] {file_name} já existe e está atualizado. Pulando...")
        return {'status': 'skipped', 'file': file_name}
    
    try:
        thread_safe_print(f"[Thread {thread_id}] Iniciando download: {file_name}")
        
        # Usar requests com stream para melhor controle
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        block_size = 8192
        downloaded = 0
        
        with open(file_path, 'wb') as file:
            for chunk in response.iter_content(block_size):
                if chunk:
                    file.write(chunk)
                    downloaded += len(chunk)
                    
                    # Atualizar progresso a cada 10%
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        if int(percent) % 10 == 0 and int(percent) != int((downloaded - len(chunk)) / total_size * 100):
                            thread_safe_print(f"[Thread {thread_id}] {file_name}: {percent:.0f}% concluído")
        
        thread_safe_print(f"[Thread {thread_id}] ✓ {file_name} baixado com sucesso!")
        return {'status': 'success', 'file': file_name}
        
    except requests.exceptions.RequestException as e:
        thread_safe_print(f"[Thread {thread_id}] ✗ Erro ao baixar {file_name}: {str(e)}")
        return {'status': 'error', 'file': file_name, 'error': str(e)}

def download_files_parallel(files_list, base_url, output_path, max_workers=5):
    """
    Baixa arquivos em paralelo usando ThreadPoolExecutor
    
    Args:
        files_list: Lista de nomes de arquivos para baixar
        base_url: URL base da Receita Federal
        output_path: Diretório de saída
        max_workers: Número máximo de downloads simultâneos (padrão: 5)
    
    Returns:
        Estatísticas do download
    """
    print(f"\n{'='*60}")
    print(f"Iniciando download paralelo com {max_workers} threads")
    print(f"Total de arquivos: {len(files_list)}")
    print(f"{'='*60}\n")
    
    start_time = time.time()
    results = {'success': [], 'error': [], 'skipped': []}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Criar futures para cada download
        future_to_file = {}
        for i, file_name in enumerate(files_list):
            url = base_url + file_name
            thread_id = i % max_workers + 1
            future = executor.submit(download_file_with_progress, url, output_path, file_name, thread_id)
            future_to_file[future] = file_name
        
        # Processar conforme completam
        completed = 0
        for future in as_completed(future_to_file):
            completed += 1
            file_name = future_to_file[future]
            try:
                result = future.result()
                results[result['status']].append(result['file'])
                
                # Mostrar progresso geral
                thread_safe_print(f"\n[PROGRESSO GERAL] {completed}/{len(files_list)} arquivos processados")
                
            except Exception as exc:
                thread_safe_print(f'Arquivo {file_name} gerou exceção: {exc}')
                results['error'].append(file_name)
    
    # Estatísticas finais
    elapsed_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"DOWNLOAD CONCLUÍDO!")
    print(f"{'='*60}")
    print(f"Tempo total: {elapsed_time:.2f} segundos ({elapsed_time/60:.2f} minutos)")
    print(f"✓ Sucesso: {len(results['success'])} arquivos")
    print(f"⊗ Pulados (já existentes): {len(results['skipped'])} arquivos")
    print(f"✗ Erros: {len(results['error'])} arquivos")
    
    if results['error']:
        print(f"\nArquivos com erro:")
        for f in results['error']:
            print(f"  - {f}")
        print(f"\nVocê pode tentar baixar estes arquivos novamente rodando o script.")
    
    print(f"{'='*60}\n")
    
    return results

def makedirs(path):
    '''
    cria path caso seja necessario
    '''
    if not os.path.exists(path):
        os.makedirs(path)

def to_sql(dataframe, **kwargs):
    '''
    Quebra em pedacos a tarefa de inserir registros no banco
    '''
    size = 4096  #TODO param
    total = len(dataframe)
    name = kwargs.get('name')

    def chunker(df):
        return (df[i:i + size] for i in range(0, len(df), size))

    for i, df in enumerate(chunker(dataframe)):
        df.to_sql(**kwargs)
        index = i * size
        percent = (index * 100) / total
        progress = f'{name} {percent:.2f}% {index:0{len(str(total))}}/{total}'
        sys.stdout.write(f'\r{progress}')
    sys.stdout.write('\n')

def reconnect_database(db_host, db_port, db_user, db_password, db_name):
    """
    Reconecta ao banco de dados quando a conexão é perdida
    """
    try:
        # Fechar conexões antigas se existirem
        try:
            cur.close()
            conn.close()
            engine.dispose()
        except:
            pass
        
        # Criar nova conexão
        encoded_password = urllib.parse.quote_plus(db_password)
        connection_string = f'postgresql+psycopg2://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}'
        engine = create_engine(
            connection_string,
            connect_args={
                "connect_timeout": 10,
                "application_name": "ETL_CNPJ"
            },
            pool_pre_ping=True,
            pool_recycle=3600
        )
        
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            dbname=db_name
        )
        cur = conn.cursor()
        
        print("✅ Reconexão com banco de dados estabelecida!")
        return engine, conn, cur
        
    except Exception as e:
        print(f"❌ Erro ao reconectar: {e}")
        return None, None, None

def test_database_connection(db_host, db_port, db_user, db_password, db_name):
    """
    Testa a conexão com o banco de dados antes de processar
    """
    print("🔄 Testando conexão com o banco de dados...")
    
    # Primeiro teste: verificar se consegue conectar no postgres padrão
    try:
        conn_test = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            dbname='postgres',
            connect_timeout=10
        )
        conn_test.close()
        print("✅ Conexão básica estabelecida!")
    except Exception as e:
        print(f"❌ Erro na conexão básica: {e}")
        print(f"\n🔧 Verifique:")
        print(f"   1. Host: {db_host}:{db_port}")
        print(f"   2. Usuário/senha estão corretos")
        print(f"   3. PostgreSQL está rodando")
        print(f"   4. Firewall permite conexão")
        return None
    
    # Segundo teste: verificar se o banco existe
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            dbname='postgres'
        )
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        exists = cur.fetchone()
        
        if not exists:
            print(f"🔄 Banco '{db_name}' não existe. Criando...")
            conn.autocommit = True
            cur.execute(f'CREATE DATABASE "{db_name}"')
            print(f"✅ Banco '{db_name}' criado com sucesso!")
        
        conn.close()
    except Exception as e:
        print(f"⚠️  Aviso ao verificar/criar banco: {e}")
    
    # Terceiro teste: conectar no banco específico
    try:
        encoded_password = urllib.parse.quote_plus(db_password)
        connection_string = f'postgresql+psycopg2://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}'
        engine = create_engine(
            connection_string,
            connect_args={
                "connect_timeout": 10,
                "application_name": "ETL_CNPJ"
            },
            pool_pre_ping=True,
            pool_recycle=3600
        )
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"✅ Conexão com '{db_name}' estabelecida!")
            print(f"📊 PostgreSQL: {version.split(',')[0]}")
            return engine
            
    except Exception as e:
        print(f"❌ Erro na conexão SQLAlchemy: {e}")
        return None

def load_env_config():
    """
    Carrega configurações do arquivo .env com validação
    """
    current_path = pathlib.Path().resolve()
    dotenv_path = os.path.join(current_path, '.env')
    
    if not os.path.isfile(dotenv_path):
        print('❌ Arquivo .env não encontrado no diretório atual.')
        print('🔍 Especifique o local do seu arquivo de configuração ".env":')
        print('   Exemplo: C:\\...\\Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ\\')
        local_env = input('Caminho: ')
        dotenv_path = os.path.join(local_env, '.env')
        
        if not os.path.isfile(dotenv_path):
            print(f'❌ Arquivo .env não encontrado em: {dotenv_path}')
            return None
    
    print(f"🔄 Carregando configurações de: {dotenv_path}")
    load_dotenv(dotenv_path=dotenv_path)
    
    # Validar variáveis obrigatórias
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD', 'DB_NAME', 
                     'OUTPUT_FILES_PATH', 'EXTRACTED_FILES_PATH']
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Variáveis obrigatórias não encontradas no .env:")
        for var in missing_vars:
            print(f"   - {var}")
        print(f"\n📝 Exemplo de arquivo .env:")
        print(f"   DB_HOST=localhost")
        print(f"   DB_PORT=5432")
        print(f"   DB_USER=postgres")
        print(f"   DB_PASSWORD=sua_senha")
        print(f"   DB_NAME=Dados_RFB")
        print(f"   OUTPUT_FILES_PATH=./output")
        print(f"   EXTRACTED_FILES_PATH=./files")
        return None
    
    return dotenv_path

# ===============================================
# INÍCIO DO SCRIPT PRINCIPAL
# ===============================================

print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       ETL DADOS PÚBLICOS CNPJ - RECEITA FEDERAL             ║
║                                                                              ║
║  🔄 Pipeline ETL para dados públicos de CNPJ da Receita Federal             ║
║  📊 Processa ~50 milhões de empresas em PostgreSQL                          ║
║                                                                              ║
║  Desenvolvido por: Victor Beppler                                           ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")

# CARREGAR CONFIGURAÇÕES DO .env
dotenv_path = load_env_config()
if not dotenv_path:
    print("❌ Não foi possível carregar as configurações. Encerrando...")
    sys.exit(1)

# CONFIGURAÇÕES DE DOWNLOAD PARALELO
MAX_DOWNLOAD_WORKERS = 5  # Número de downloads simultâneos (recomendado: 3-10)
DOWNLOAD_TIMEOUT = 1800  # Timeout para cada download em segundos

print(f"\n⚙️  Configuração de Download:")
print(f"    - Downloads simultâneos: {MAX_DOWNLOAD_WORKERS}")
print(f"    - Timeout por arquivo: {DOWNLOAD_TIMEOUT}s")
print(f"    - Modo: {'Rápido' if MAX_DOWNLOAD_WORKERS >= 5 else 'Conservador'}")

# CONFIGURAR PERÍODO DE DADOS
YEAR = 2025
MONTH = 8
period = f"{YEAR:04d}-{MONTH:02d}"
dados_rf = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{period}/"

print(f"\n📅 Buscando dados do período: {period}")
print(f"🌐 URL: {dados_rf}")

# CONFIGURAR DIRETÓRIOS
try:
    output_files = os.getenv('OUTPUT_FILES_PATH')
    extracted_files = os.getenv('EXTRACTED_FILES_PATH')
    
    makedirs(output_files)
    makedirs(extracted_files)
    
    print(f'\n📁 Diretórios configurados:')
    print(f'    - Arquivos baixados: {output_files}')
    print(f'    - Arquivos extraídos: {extracted_files}')
except Exception as e:
    print(f'❌ Erro na configuração dos diretórios: {e}')
    print('   Verifique o arquivo .env')
    sys.exit(1)

# CONFIGURAR CONEXÃO COM BANCO DE DADOS
try:
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    
    print(f'\n💾 Configuração do banco:')
    print(f'    - Host: {db_host}:{db_port}')
    print(f'    - Banco: {db_name}')
    print(f'    - Usuário: {db_user}')
    
    # TESTAR CONEXÃO
    engine = test_database_connection(db_host, db_port, db_user, db_password, db_name)
    if not engine:
        print("\n❌ Não foi possível estabelecer conexão com o banco. Encerrando...")
        sys.exit(1)
        
    # Criar conexão psycopg2 para comandos DDL
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        dbname=db_name
    )
    cur = conn.cursor()
    
except Exception as e:
    print(f'❌ Erro na configuração do banco de dados: {e}')
    sys.exit(1)

# ===============================================
# BUSCAR ARQUIVOS DISPONÍVEIS
# ===============================================

try:
    print(f"\n🔍 Buscando arquivos disponíveis...")
    raw_html = urllib.request.urlopen(dados_rf)
    raw_html = raw_html.read()
except urllib.error.HTTPError as e:
    print(f"❌ Erro ao acessar a URL: {e}")
    print(f"   Verifique se o período {period} está disponível.")
    print("   Os dados geralmente são disponibilizados mensalmente.")
    sys.exit(1)

# Formatar página e converter em string
page_items = bs.BeautifulSoup(raw_html, 'lxml')
html_str = str(page_items)

# Obter arquivos
Files = []

# Buscar links diretos
for link in page_items.find_all('a', href=True):
    href = link['href']
    if href.endswith('.zip'):
        if not href.startswith('http'):
            file_name = href.split('/')[-1]
            Files.append(file_name)
        else:
            file_name = href.split('/')[-1]
            Files.append(file_name)

# Se não encontrou, tenta método alternativo
if not Files:
    print("🔄 Tentando método alternativo de busca...")
    text = '.zip'
    for m in re.finditer(text, html_str):
        i_start = m.start()-40
        i_end = m.end()
        i_loc = html_str[i_start:i_end].find('href=')+6
        file_path = html_str[i_start+i_loc:i_end]
        
        if '.zip' in file_path:
            file_name = file_path
            if '"' in file_name:
                file_name = file_name.split('"')[0]
            if '>' in file_name:
                file_name = file_name.split('>')[0]
            
            file_name = file_name.split('/')[-1]
            
            if file_name.endswith('.zip') and not file_name.startswith('<'):
                Files.append(file_name)

# Remover duplicatas e ordenar
Files = sorted(list(set(Files)))

if not Files:
    print("⚠️  AVISO: Nenhum arquivo .zip encontrado na página.")
    print("   Verifique a URL ou tente outro período.")
    sys.exit(1)

print(f'✅ Encontrados {len(Files)} arquivos para download')

# ===============================================
# DOWNLOAD PARALELO DOS ARQUIVOS
# ===============================================

print(f"\n{'='*80}")
print(f"🚀 INICIANDO DOWNLOAD DOS ARQUIVOS")
print(f"{'='*80}")

download_results = download_files_parallel(
    files_list=Files,
    base_url=dados_rf,
    output_path=output_files,
    max_workers=MAX_DOWNLOAD_WORKERS
)

# Verificar se houve muitos erros
if len(download_results['error']) > len(Files) * 0.5:  # Mais de 50% de erro
    print("\n⚠️  ATENÇÃO: Muitos arquivos falharam no download.")
    print("📋 Possíveis causas:")
    print("    1. Conexão instável com a internet")
    print("    2. Servidor da Receita Federal sobrecarregado")
    print("    3. Período não disponível")
    print("\n💡 Tente executar novamente ou reduza MAX_DOWNLOAD_WORKERS para 2 ou 3.")
    
    resposta = input("\n❓ Deseja continuar mesmo assim? (s/n): ")
    if resposta.lower() != 's':
        print("❌ Processo cancelado.")
        sys.exit(1)

# ===============================================
# EXTRAÇÃO DOS ARQUIVOS
# ===============================================

print(f"\n{'='*80}")
print("📦 INICIANDO EXTRAÇÃO DOS ARQUIVOS")
print(f"{'='*80}")

extraction_start = time.time()
extracted_count = 0
extraction_errors = []

for i, file_name in enumerate(Files, 1):
    try:
        print(f'📂 Descompactando arquivo {i}/{len(Files)}: {file_name}')
        full_path = os.path.join(output_files, file_name)
        
        if not os.path.exists(full_path):
            print(f"    ⊗ Arquivo não encontrado (provavelmente não foi baixado): {file_name}")
            continue
            
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            zip_ref.extractall(extracted_files)
        extracted_count += 1
        print(f"    ✅ Extraído com sucesso!")
    except Exception as e:
        print(f"    ❌ Erro ao descompactar {file_name}: {e}")
        extraction_errors.append(file_name)
        continue

extraction_time = time.time() - extraction_start
print(f"\n{'='*80}")
print(f"✅ EXTRAÇÃO CONCLUÍDA!")
print(f"⏱️  Tempo de extração: {extraction_time:.2f} segundos")
print(f"📊 Arquivos extraídos: {extracted_count}/{len(Files)}")
if extraction_errors:
    print(f"❌ Erros de extração: {len(extraction_errors)}")
print(f"{'='*80}")

# ===============================================
# PROCESSAR E INSERIR DADOS NO BANCO
# ===============================================

print(f"\n{'='*80}")
print("💾 INICIANDO PROCESSAMENTO E CARGA NO BANCO DE DADOS")
print(f"{'='*80}")

insert_start = time.time()

# Listar arquivos extraídos
Items = [name for name in os.listdir(extracted_files) if name.endswith('')]

# Separar arquivos por tipo
arquivos_empresa = []
arquivos_estabelecimento = []
arquivos_socios = []
arquivos_simples = []
arquivos_cnae = []
arquivos_moti = []
arquivos_munic = []
arquivos_natju = []
arquivos_pais = []
arquivos_quals = []

for item in Items:
    if 'EMPRE' in item:
        arquivos_empresa.append(item)
    elif 'ESTABELE' in item:
        arquivos_estabelecimento.append(item)
    elif 'SOCIO' in item:
        arquivos_socios.append(item)
    elif 'SIMPLES' in item:
        arquivos_simples.append(item)
    elif 'CNAE' in item:
        arquivos_cnae.append(item)
    elif 'MOTI' in item:
        arquivos_moti.append(item)
    elif 'MUNIC' in item:
        arquivos_munic.append(item)
    elif 'NATJU' in item:
        arquivos_natju.append(item)
    elif 'PAIS' in item:
        arquivos_pais.append(item)
    elif 'QUALS' in item:
        arquivos_quals.append(item)

# Mostrar resumo dos arquivos encontrados
print("\n📋 Resumo dos arquivos encontrados:")
print(f"    - Empresa: {len(arquivos_empresa)} arquivo(s)")
print(f"    - Estabelecimento: {len(arquivos_estabelecimento)} arquivo(s)")
print(f"    - Sócios: {len(arquivos_socios)} arquivo(s)")
print(f"    - Simples: {len(arquivos_simples)} arquivo(s)")
print(f"    - CNAE: {len(arquivos_cnae)} arquivo(s)")
print(f"    - Motivos: {len(arquivos_moti)} arquivo(s)")
print(f"    - Municípios: {len(arquivos_munic)} arquivo(s)")
print(f"    - Natureza Jurídica: {len(arquivos_natju)} arquivo(s)")
print(f"    - País: {len(arquivos_pais)} arquivo(s)")
print(f"    - Qualificação: {len(arquivos_quals)} arquivo(s)")

# ===============================================
# PROCESSAR ARQUIVOS DE EMPRESA
# ===============================================

empresa_insert_start = time.time()
print(f"\n{'='*60}")
print("🏢 PROCESSANDO ARQUIVOS DE EMPRESA")
print(f"{'='*60}")

# Drop table antes do insert
try:
    cur.execute('DROP TABLE IF EXISTS "empresa";')
    conn.commit()
    print("🗑️  Tabela 'empresa' removida (se existia)")
except Exception as e:
    print(f"⚠️  Aviso ao remover tabela empresa: {e}")
    # Reconectar se necessário
    engine, conn, cur = reconnect_database(db_host, db_port, db_user, db_password, db_name)

for e, arquivo in enumerate(arquivos_empresa, 1):
    print(f'📄 Processando arquivo {e}/{len(arquivos_empresa)}: {arquivo}')
    try:
        # Limpar memória
        try:
            del empresa
            gc.collect()
        except:
            pass

        empresa_dtypes = {0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: object}
        extracted_file_path = os.path.join(extracted_files, arquivo)

        empresa = pd.read_csv(
            filepath_or_buffer=extracted_file_path,
            sep=';',
            skiprows=0,
            header=None,
            dtype=empresa_dtypes,
            encoding='latin-1',
        )

        # Tratamento do arquivo
        empresa = empresa.reset_index()
        del empresa['index']

        # Renomear colunas
        empresa.columns = ['cnpj_basico', 'razao_social', 'natureza_juridica', 
                          'qualificacao_responsavel', 'capital_social', 'porte_empresa', 
                          'ente_federativo_responsavel']

        # Tratar capital social
        empresa['capital_social'] = empresa['capital_social'].apply(lambda x: x.replace(',','.'))
        empresa['capital_social'] = empresa['capital_social'].astype(float)

        # Gravar dados no banco
        print(f"    💾 Inserindo {len(empresa)} registros no banco...")
        to_sql(empresa, name='empresa', con=engine, if_exists='append', index=False)
        print(f'    ✅ Arquivo {arquivo} inserido com sucesso!')

    except Exception as error:
        print(f'    ❌ Erro ao processar {arquivo}: {error}')
        continue

# Limpar memória
try:
    del empresa
    gc.collect()
except:
    pass

empresa_insert_end = time.time()
empresa_tempo_insert = round(empresa_insert_end - empresa_insert_start)
print(f'\n⏱️  Tempo de processamento de empresas: {empresa_tempo_insert} segundos')

# ===============================================
# PROCESSAR ARQUIVOS DE ESTABELECIMENTO
# ===============================================

estabelecimento_insert_start = time.time()
print(f"\n{'='*60}")
print("🏪 PROCESSANDO ARQUIVOS DE ESTABELECIMENTO")
print(f"{'='*60}")

# Drop table antes do insert
try:
    cur.execute('DROP TABLE IF EXISTS "estabelecimento";')
    conn.commit()
    print("🗑️  Tabela 'estabelecimento' removida (se existia)")
except Exception as e:
    print(f"⚠️  Aviso ao remover tabela estabelecimento: {e}")
    # Reconectar se necessário
    engine, conn, cur = reconnect_database(db_host, db_port, db_user, db_password, db_name)

print(f'📊 Total de arquivos de estabelecimento: {len(arquivos_estabelecimento)}')

for e, arquivo in enumerate(arquivos_estabelecimento, 1):
    print(f'📄 Processando arquivo {e}/{len(arquivos_estabelecimento)}: {arquivo}')
    try:
        # Limpar memória
        try:
            del estabelecimento
            gc.collect()
        except:
            pass

        estabelecimento_dtypes = {0: object, 1: object, 2: object, 3: 'Int32', 4: object, 5: 'Int32', 6: 'Int32',
                                  7: 'Int32', 8: object, 9: object, 10: 'Int32', 11: 'Int32', 12: object, 13: object,
                                  14: object, 15: object, 16: object, 17: object, 18: object, 19: object,
                                  20: 'Int32', 21: object, 22: object, 23: object, 24: object, 25: object,
                                  26: object, 27: object, 28: object, 29: 'Int32'}
        
        extracted_file_path = os.path.join(extracted_files, arquivo)

        # Processar em lotes devido ao tamanho
        NROWS = 2000000
        part = 0
        
        while True:
            print(f"    📦 Processando lote {part + 1}...")
            
            estabelecimento = pd.read_csv(
                filepath_or_buffer=extracted_file_path,
                sep=';',
                nrows=NROWS,
                skiprows=NROWS * part,
                header=None,
                dtype=estabelecimento_dtypes,
                encoding='latin-1',
            )

            # Se chegou ao fim do arquivo
            if len(estabelecimento) == 0:
                break

            # Tratamento do arquivo
            estabelecimento = estabelecimento.reset_index()
            del estabelecimento['index']
            gc.collect()

            # Renomear colunas
            estabelecimento.columns = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial',
                                       'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
                                       'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais',
                                       'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
                                       'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro',
                                       'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2',
                                       'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_situacao_especial']

            # Gravar dados no banco
            print(f"        💾 Inserindo {len(estabelecimento)} registros...")
            to_sql(estabelecimento, name='estabelecimento', con=engine, if_exists='append', index=False)
            print(f'        ✅ Lote {part + 1} inserido com sucesso!')
            
            if len(estabelecimento) < NROWS:
                break
                
            part += 1

    except Exception as error:
        print(f'    ❌ Erro ao processar {arquivo}: {error}')
        continue

# Limpar memória
try:
    del estabelecimento
    gc.collect()
except:
    pass

estabelecimento_insert_end = time.time()
estabelecimento_tempo_insert = round(estabelecimento_insert_end - estabelecimento_insert_start)
print(f'\n⏱️  Tempo de processamento de estabelecimentos: {estabelecimento_tempo_insert} segundos')

# ===============================================
# PROCESSAR ARQUIVOS DE SÓCIOS
# ===============================================

socios_insert_start = time.time()
print(f"\n{'='*60}")
print("👥 PROCESSANDO ARQUIVOS DE SÓCIOS")
print(f"{'='*60}")

# Drop table antes do insert
try:
    cur.execute('DROP TABLE IF EXISTS "socios";')
    conn.commit()
    print("🗑️  Tabela 'socios' removida (se existia)")
except Exception as e:
    print(f"⚠️  Aviso ao remover tabela socios: {e}")
    # Reconectar se necessário
    engine, conn, cur = reconnect_database(db_host, db_port, db_user, db_password, db_name)

for e, arquivo in enumerate(arquivos_socios, 1):
    print(f'📄 Processando arquivo {e}/{len(arquivos_socios)}: {arquivo}')
    try:
        # Limpar memória
        try:
            del socios
            gc.collect()
        except:
            pass

        socios_dtypes = {0: object, 1: 'Int32', 2: object, 3: object, 4: 'Int32', 5: 'Int32', 6: 'Int32',
                         7: object, 8: object, 9: 'Int32', 10: 'Int32'}
        
        extracted_file_path = os.path.join(extracted_files, arquivo)
        
        socios = pd.read_csv(
            filepath_or_buffer=extracted_file_path,
            sep=';',
            skiprows=0,
            header=None,
            dtype=socios_dtypes,
            encoding='latin-1',
        )

        # Tratamento do arquivo
        socios = socios.reset_index()
        del socios['index']

        # Renomear colunas
        socios.columns = ['cnpj_basico', 'identificador_socio', 'nome_socio_razao_social', 'cpf_cnpj_socio',
                          'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal',
                          'nome_do_representante', 'qualificacao_representante_legal', 'faixa_etaria']

        # Gravar dados no banco
        print(f"    💾 Inserindo {len(socios)} registros no banco...")
        to_sql(socios, name='socios', con=engine, if_exists='append', index=False)
        print(f'    ✅ Arquivo {arquivo} inserido com sucesso!')

    except Exception as error:
        print(f'    ❌ Erro ao processar {arquivo}: {error}')
        continue

# Limpar memória
try:
    del socios
    gc.collect()
except:
    pass

socios_insert_end = time.time()
socios_tempo_insert = round(socios_insert_end - socios_insert_start)
print(f'\n⏱️  Tempo de processamento de sócios: {socios_tempo_insert} segundos')

# ===============================================
# PROCESSAR ARQUIVOS DE SIMPLES
# ===============================================

if arquivos_simples:
    simples_insert_start = time.time()
    print(f"\n{'='*60}")
    print("📊 PROCESSANDO ARQUIVOS DO SIMPLES NACIONAL")
    print(f"{'='*60}")

    # Drop table antes do insert
    try:
        cur.execute('DROP TABLE IF EXISTS "simples";')
        conn.commit()
        print("🗑️  Tabela 'simples' removida (se existia)")
    except Exception as e:
        print(f"⚠️  Aviso ao remover tabela simples: {e}")
        engine, conn, cur = reconnect_database(db_host, db_port, db_user, db_password, db_name)

    for e, arquivo in enumerate(arquivos_simples, 1):
        print(f'📄 Processando arquivo {e}/{len(arquivos_simples)}: {arquivo}')
        try:
            # Verificar tamanho do arquivo
            extracted_file_path = os.path.join(extracted_files, arquivo)
            simples_length = sum(1 for line in open(extracted_file_path, "r", encoding='latin-1'))
            print(f'    📊 Linhas no arquivo: {simples_length}')
            
            tamanho_das_partes = 1000000
            partes = max(1, simples_length // tamanho_das_partes + 1)
            print(f'    📦 Arquivo será dividido em {partes} parte(s)')
            
            simples_dtypes = {0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: 'Int32'}
            
            for i in range(partes):
                print(f'        📦 Processando parte {i+1}/{partes}...')
                
                simples = pd.read_csv(
                    filepath_or_buffer=extracted_file_path,
                    sep=';',
                    nrows=tamanho_das_partes,
                    skiprows=tamanho_das_partes * i,
                    header=None,
                    dtype=simples_dtypes,
                    encoding='latin-1',
                )
                
                if len(simples) == 0:
                    break
                
                # Tratamento do arquivo
                simples = simples.reset_index()
                del simples['index']
                
                # Renomear colunas
                simples.columns = ['cnpj_basico', 'opcao_pelo_simples', 'data_opcao_simples',
                                  'data_exclusao_simples', 'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei']
                
                # Gravar dados no banco
                print(f"            💾 Inserindo {len(simples)} registros...")
                to_sql(simples, name='simples', con=engine, if_exists='append', index=False)
                print(f'            ✅ Parte {i+1} inserida com sucesso!')
                
                # Limpar memória
                del simples
                gc.collect()
                
        except Exception as error:
            print(f'    ❌ Erro ao processar {arquivo}: {error}')
            continue
    
    simples_insert_end = time.time()
    simples_tempo_insert = round(simples_insert_end - simples_insert_start)
    print(f'\n⏱️  Tempo de processamento do Simples: {simples_tempo_insert} segundos')

# ===============================================
# PROCESSAR ARQUIVOS DE CNAE
# ===============================================

if arquivos_cnae:
    cnae_insert_start = time.time()
    print(f"\n{'='*60}")
    print("🏭 PROCESSANDO ARQUIVOS DE CNAE")
    print(f"{'='*60}")

    try:
        cur.execute('DROP TABLE IF EXISTS "cnae";')
        conn.commit()
        print("🗑️  Tabela 'cnae' removida (se existia)")
    except Exception as e:
        print(f"⚠️  Aviso ao remover tabela cnae: {e}")
        engine, conn, cur = reconnect_database(db_host, db_port, db_user, db_password, db_name)

    for e, arquivo in enumerate(arquivos_cnae, 1):
        print(f'📄 Processando arquivo {e}/{len(arquivos_cnae)}: {arquivo}')
        try:
            extracted_file_path = os.path.join(extracted_files, arquivo)
            cnae = pd.read_csv(
                filepath_or_buffer=extracted_file_path,
                sep=';',
                skiprows=0,
                header=None,
                dtype='object',
                encoding='latin-1'
            )
            
            cnae = cnae.reset_index()
            del cnae['index']
            
            cnae.columns = ['codigo', 'descricao']
            
            print(f"    💾 Inserindo {len(cnae)} registros no banco...")
            to_sql(cnae, name='cnae', con=engine, if_exists='append', index=False)
            print(f'    ✅ Arquivo {arquivo} inserido com sucesso!')
            
        except Exception as error:
            print(f'    ❌ Erro ao processar {arquivo}: {error}')
            continue
    
    cnae_insert_end = time.time()
    cnae_tempo_insert = round(cnae_insert_end - cnae_insert_start)
    print(f'\n⏱️  Tempo de processamento de CNAE: {cnae_tempo_insert} segundos')

# ===============================================
# PROCESSAR ARQUIVOS DE MOTIVOS
# ===============================================

if arquivos_moti:
    moti_insert_start = time.time()
    print(f"\n{'='*60}")
    print("📋 PROCESSANDO ARQUIVOS DE MOTIVOS")
    print(f"{'='*60}")

    try:
        cur.execute('DROP TABLE IF EXISTS "moti";')
        conn.commit()
        print("🗑️  Tabela 'moti' removida (se existia)")
    except Exception as e:
        print(f"⚠️  Aviso ao remover tabela moti: {e}")
        engine, conn, cur = reconnect_database(db_host, db_port, db_user, db_password, db_name)

    for e, arquivo in enumerate(arquivos_moti, 1):
        print(f'📄 Processando arquivo {e}/{len(arquivos_moti)}: {arquivo}')
        try:
            moti_dtypes = {0: 'Int32', 1: object}
            extracted_file_path = os.path.join(extracted_files, arquivo)
            moti = pd.read_csv(
                filepath_or_buffer=extracted_file_path,
                sep=';',
                skiprows=0,
                header=None,
                dtype=moti_dtypes,
                encoding='latin-1'
            )
            
            moti = moti.reset_index()
            del moti['index']
            
            moti.columns = ['codigo', 'descricao']
            
            print(f"    💾 Inserindo {len(moti)} registros no banco...")
            to_sql(moti, name='moti', con=engine, if_exists='append', index=False)
            print(f'    ✅ Arquivo {arquivo} inserido com sucesso!')
            
        except Exception as error:
            print(f'    ❌ Erro ao processar {arquivo}: {error}')
            continue
    
    moti_insert_end = time.time()
    moti_tempo_insert = round(moti_insert_end - moti_insert_start)
    print(f'\n⏱️  Tempo de processamento de Motivos: {moti_tempo_insert} segundos')

# ===============================================
# PROCESSAR ARQUIVOS DE MUNICÍPIOS
# ===============================================

if arquivos_munic:
    munic_insert_start = time.time()
    print(f"\n{'='*60}")
    print("🏙️ PROCESSANDO ARQUIVOS DE MUNICÍPIOS")
    print(f"{'='*60}")

    try:
        cur.execute('DROP TABLE IF EXISTS "munic";')
        conn.commit()
        print("🗑️  Tabela 'munic' removida (se existia)")
    except Exception as e:
        print(f"⚠️  Aviso ao remover tabela munic: {e}")
        engine, conn, cur = reconnect_database(db_host, db_port, db_user, db_password, db_name)

    for e, arquivo in enumerate(arquivos_munic, 1):
        print(f'📄 Processando arquivo {e}/{len(arquivos_munic)}: {arquivo}')
        try:
            munic_dtypes = {0: 'Int32', 1: object}
            extracted_file_path = os.path.join(extracted_files, arquivo)
            munic = pd.read_csv(
                filepath_or_buffer=extracted_file_path,
                sep=';',
                skiprows=0,
                header=None,
                dtype=munic_dtypes,
                encoding='latin-1'
            )
            
            munic = munic.reset_index()
            del munic['index']
            
            munic.columns = ['codigo', 'descricao']
            
            print(f"    💾 Inserindo {len(munic)} registros no banco...")
            to_sql(munic, name='munic', con=engine, if_exists='append', index=False)
            print(f'    ✅ Arquivo {arquivo} inserido com sucesso!')
            
        except Exception as error:
            print(f'    ❌ Erro ao processar {arquivo}: {error}')
            continue
    
    munic_insert_end = time.time()
    munic_tempo_insert = round(munic_insert_end - munic_insert_start)
    print(f'\n⏱️  Tempo de processamento de Municípios: {munic_tempo_insert} segundos')

# ===============================================
# PROCESSAR ARQUIVOS DE NATUREZA JURÍDICA
# ===============================================

if arquivos_natju:
    natju_insert_start = time.time()
    print(f"\n{'='*60}")
    print("⚖️ PROCESSANDO ARQUIVOS DE NATUREZA JURÍDICA")
    print(f"{'='*60}")

    try:
        cur.execute('DROP TABLE IF EXISTS "natju";')
        conn.commit()
        print("🗑️  Tabela 'natju' removida (se existia)")
    except Exception as e:
        print(f"⚠️  Aviso ao remover tabela natju: {e}")
        engine, conn, cur = reconnect_database(db_host, db_port, db_user, db_password, db_name)

    for e, arquivo in enumerate(arquivos_natju, 1):
        print(f'📄 Processando arquivo {e}/{len(arquivos_natju)}: {arquivo}')
        try:
            natju_dtypes = {0: 'Int32', 1: object}
            extracted_file_path = os.path.join(extracted_files, arquivo)
            natju = pd.read_csv(
                filepath_or_buffer=extracted_file_path,
                sep=';',
                skiprows=0,
                header=None,
                dtype=natju_dtypes,
                encoding='latin-1'
            )
            
            natju = natju.reset_index()
            del natju['index']
            
            natju.columns = ['codigo', 'descricao']
            
            print(f"    💾 Inserindo {len(natju)} registros no banco...")
            to_sql(natju, name='natju', con=engine, if_exists='append', index=False)
            print(f'    ✅ Arquivo {arquivo} inserido com sucesso!')
            
        except Exception as error:
            print(f'    ❌ Erro ao processar {arquivo}: {error}')
            continue
    
    natju_insert_end = time.time()
    natju_tempo_insert = round(natju_insert_end - natju_insert_start)
    print(f'\n⏱️  Tempo de processamento de Natureza Jurídica: {natju_tempo_insert} segundos')

# ===============================================
# PROCESSAR ARQUIVOS DE PAÍS
# ===============================================

if arquivos_pais:
    pais_insert_start = time.time()
    print(f"\n{'='*60}")
    print("🌍 PROCESSANDO ARQUIVOS DE PAÍS")
    print(f"{'='*60}")

    try:
        cur.execute('DROP TABLE IF EXISTS "pais";')
        conn.commit()
        print("🗑️  Tabela 'pais' removida (se existia)")
    except Exception as e:
        print(f"⚠️  Aviso ao remover tabela pais: {e}")
        engine, conn, cur = reconnect_database(db_host, db_port, db_user, db_password, db_name)

    for e, arquivo in enumerate(arquivos_pais, 1):
        print(f'📄 Processando arquivo {e}/{len(arquivos_pais)}: {arquivo}')
        try:
            pais_dtypes = {0: 'Int32', 1: object}
            extracted_file_path = os.path.join(extracted_files, arquivo)
            pais = pd.read_csv(
                filepath_or_buffer=extracted_file_path,
                sep=';',
                skiprows=0,
                header=None,
                dtype=pais_dtypes,
                encoding='latin-1'
            )
            
            pais = pais.reset_index()
            del pais['index']
            
            pais.columns = ['codigo', 'descricao']
            
            print(f"    💾 Inserindo {len(pais)} registros no banco...")
            to_sql(pais, name='pais', con=engine, if_exists='append', index=False)
            print(f'    ✅ Arquivo {arquivo} inserido com sucesso!')
            
        except Exception as error:
            print(f'    ❌ Erro ao processar {arquivo}: {error}')
            continue
    
    pais_insert_end = time.time()
    pais_tempo_insert = round(pais_insert_end - pais_insert_start)
    print(f'\n⏱️  Tempo de processamento de País: {pais_tempo_insert} segundos')

# ===============================================
# PROCESSAR ARQUIVOS DE QUALIFICAÇÃO
# ===============================================

if arquivos_quals:
    quals_insert_start = time.time()
    print(f"\n{'='*60}")
    print("👔 PROCESSANDO ARQUIVOS DE QUALIFICAÇÃO DE SÓCIOS")
    print(f"{'='*60}")

    try:
        cur.execute('DROP TABLE IF EXISTS "quals";')
        conn.commit()
        print("🗑️  Tabela 'quals' removida (se existia)")
    except Exception as e:
        print(f"⚠️  Aviso ao remover tabela quals: {e}")
        engine, conn, cur = reconnect_database(db_host, db_port, db_user, db_password, db_name)

    for e, arquivo in enumerate(arquivos_quals, 1):
        print(f'📄 Processando arquivo {e}/{len(arquivos_quals)}: {arquivo}')
        try:
            quals_dtypes = {0: 'Int32', 1: object}
            extracted_file_path = os.path.join(extracted_files, arquivo)
            quals = pd.read_csv(
                filepath_or_buffer=extracted_file_path,
                sep=';',
                skiprows=0,
                header=None,
                dtype=quals_dtypes,
                encoding='latin-1'
            )
            
            quals = quals.reset_index()
            del quals['index']
            
            quals.columns = ['codigo', 'descricao']
            
            print(f"    💾 Inserindo {len(quals)} registros no banco...")
            to_sql(quals, name='quals', con=engine, if_exists='append', index=False)
            print(f'    ✅ Arquivo {arquivo} inserido com sucesso!')
            
        except Exception as error:
            print(f'    ❌ Erro ao processar {arquivo}: {error}')
            continue
    
    quals_insert_end = time.time()
    quals_tempo_insert = round(quals_insert_end - quals_insert_start)
    print(f'\n⏱️  Tempo de processamento de Qualificação: {quals_tempo_insert} segundos')

# ===============================================
# CRIAR ÍNDICES NO BANCO DE DADOS
# ===============================================

index_start = time.time()
print(f"\n{'='*60}")
print("🔍 CRIANDO ÍNDICES NO BANCO DE DADOS")
print(f"{'='*60}")

try:
    # Reconectar para garantir conexão estável
    engine, conn, cur = reconnect_database(db_host, db_port, db_user, db_password, db_name)
    
    print("📊 Criando índices para otimizar consultas...")
    
    # Criar índices para as tabelas principais
    indices_sql = """
    CREATE INDEX IF NOT EXISTS empresa_cnpj ON empresa(cnpj_basico);
    CREATE INDEX IF NOT EXISTS estabelecimento_cnpj ON estabelecimento(cnpj_basico);
    CREATE INDEX IF NOT EXISTS socios_cnpj ON socios(cnpj_basico);
    """
    
    # Adicionar índice para simples se existir
    if arquivos_simples:
        indices_sql += "CREATE INDEX IF NOT EXISTS simples_cnpj ON simples(cnpj_basico);"
    
    # Executar criação de índices
    for sql_command in indices_sql.strip().split(';'):
        if sql_command.strip():
            try:
                cur.execute(sql_command.strip() + ';')
                conn.commit()
            except Exception as e:
                print(f"⚠️  Aviso ao criar índice: {e}")
    
    print("✅ Índices criados com sucesso nas tabelas:")
    print("    - empresa (cnpj_basico)")
    print("    - estabelecimento (cnpj_basico)")
    print("    - socios (cnpj_basico)")
    if arquivos_simples:
        print("    - simples (cnpj_basico)")
        
except Exception as e:
    print(f"❌ Erro ao criar índices: {e}")

index_end = time.time()
index_time = round(index_end - index_start)
print(f'\n⏱️  Tempo para criar os índices: {index_time} segundos')

# ===============================================
# RESUMO FINAL
# ===============================================

total_time = time.time() - insert_start

print(f"""
{'='*80}
🎉 PROCESSO 100% FINALIZADO!
{'='*80}

📊 ESTATÍSTICAS FINAIS:
    📅 Período importado: {period}
    📥 Arquivos baixados: {len(download_results['success']) + len(download_results['skipped'])}/{len(Files)}
    ⏱️  Tempo total de processamento: {total_time:.2f} segundos ({total_time/60:.2f} minutos)
    💾 Banco de dados: {db_name} em {db_host}:{db_port}

📋 TABELAS PROCESSADAS:
    ✅ Empresa: {len(arquivos_empresa)} arquivo(s)
    ✅ Estabelecimento: {len(arquivos_estabelecimento)} arquivo(s)
    ✅ Sócios: {len(arquivos_socios)} arquivo(s)
    ✅ Simples: {len(arquivos_simples)} arquivo(s)
    ✅ CNAE: {len(arquivos_cnae)} arquivo(s)
    ✅ Motivos: {len(arquivos_moti)} arquivo(s)
    ✅ Municípios: {len(arquivos_munic)} arquivo(s)
    ✅ Natureza Jurídica: {len(arquivos_natju)} arquivo(s)
    ✅ País: {len(arquivos_pais)} arquivo(s)
    ✅ Qualificação: {len(arquivos_quals)} arquivo(s)

✅ Seus dados estão prontos para uso no banco de dados!

💡 DICAS:
    🔧 Para alterar o período: modifique YEAR e MONTH no início do script
    ⚡ Para ajustar velocidade: modifique MAX_DOWNLOAD_WORKERS (1-10)
    🔄 Arquivos já baixados são automaticamente pulados em execuções futuras
    🔍 Configure seu .env para diferentes ambientes (dev/prod)

{'='*80}
""")

# Fechar conexões
try:
    cur.close()
    conn.close()
    engine.dispose()
    print("🔒 Conexões com banco de dados fechadas com sucesso")
except:
    pass