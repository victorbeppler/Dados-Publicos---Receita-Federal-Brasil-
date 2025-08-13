# 🏢 Dados Públicos CNPJ - Pipeline ETL Otimizado

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%2B-336791)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Ativo-success)](https://github.com/victorbeppler/Dados-Publicos---Receita-Federal-Brasil-.git)

**Pipeline ETL Python otimizado para dados públicos de CNPJ da Receita Federal.** Automatiza download paralelo, extração e carga de ~50 milhões de empresas em PostgreSQL com melhorias significativas de performance e robustez.

## 📋 Índice
- [Visão Geral](#-visão-geral)
- [🚀 Principais Melhorias](#-principais-melhorias)
- [Funcionalidades](#-funcionalidades)
- [Pré-requisitos](#-pré-requisitos)
- [Instalação](#-instalação)
- [Configuração](#️-configuração)
- [Uso](#-uso)
- [Estrutura do Banco](#-estrutura-do-banco-de-dados)
- [Performance](#-performance)
- [Troubleshooting](#-troubleshooting)
- [Créditos](#-créditos)

## 🎯 Visão Geral

A Receita Federal do Brasil disponibiliza mensalmente bases com dados públicos do Cadastro Nacional de Pessoas Jurídicas (CNPJ). Este projeto oferece um pipeline ETL completo e **otimizado** para:

- 📥 **Download automatizado** dos arquivos (com suporte a download paralelo)
- 📦 **Extração** dos arquivos compactados (~5GB → ~20GB)
- 🔄 **Processamento** e tratamento dos dados
- 💾 **Carga** em banco PostgreSQL estruturado e otimizado
- 🛡️ **Validação** e tratamento robusto de erros

### 📊 Dados Disponíveis
- **50+ milhões** de empresas cadastradas
- **20+ milhões** de sócios
- **Simples Nacional** e MEI
- **48 mil** estabelecimentos
- Dados históricos e situação cadastral

## 🚀 Principais Melhorias

Esta versão **otimizada** inclui melhorias significativas sobre o projeto original:

### ⚡ Performance e Robustez
- **Download Paralelo**: Até 10x mais rápido com downloads simultâneos (5 threads)
- **Validação de Integridade**: Verifica arquivos corrompidos/incompletos automaticamente
- **Smart Resume**: Retoma downloads interrompidos automaticamente
- **Tratamento de Erros**: Sistema robusto de recuperação e retry

### 🔧 Configuração e Segurança  
- **Configuração .env**: Todas as credenciais e configurações via arquivo .env
- **Validação de Conexão**: Testa banco de dados antes de processar
- **Logs Detalhados**: Acompanhamento visual com emojis e progress tracking
- **Gestão de Memória**: Otimizada para grandes volumes de dados

### 📱 Experiência do Usuário
- **Interface Visual**: Logs coloridos e informativos
- **Progress Tracking**: Acompanhamento em tempo real do processamento
- **Validação Automática**: Verifica dependências e configurações
- **Documentação Completa**: Guias detalhados e templates

## ✨ Funcionalidades

### 💼 Funcionalidades Core
- Download automático da fonte oficial
- Descompactação e validação de arquivos
- Tratamento de dados e encoding (Latin-1 → UTF-8)
- Criação automática de índices para performance
- Processamento em batch para grandes volumes
- Logs detalhados de execução

### 🔄 Funcionalidades Avançadas
- **Processamento Incremental**: Suporte para atualizações mensais
- **Configuração Flexível**: Diferentes ambientes (dev/prod)
- **Paralelização Inteligente**: Balanceamento automático de recursos
- **Backup e Recuperação**: Sistema de checkpoint para interrupções

## 🔧 Pré-requisitos

### Software Necessário
- **Python 3.8+** ([Download](https://www.python.org/downloads/))
- **PostgreSQL 14+** ([Download](https://www.postgresql.org/download/))
- **Git** (opcional, para clonar o repositório)

### Hardware Recomendado
- **RAM**: Mínimo 8GB (16GB recomendado)
- **Disco**: 50GB livres (20GB para dados + 30GB temporários)
- **CPU**: 4+ cores para processamento paralelo
- **Internet**: Conexão estável (download de ~5GB)

## 📦 Instalação

### 1. Clone o Repositório
```bash
git clone https://github.com/victorbeppler/Dados-Publicos---Receita-Federal-Brasil-.git
cd Dados-Publicos---Receita-Federal-Brasil-
```

### 2. Crie um Ambiente Virtual (Recomendado)
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

### 3. Instale as Dependências
```bash
pip install -r requirements.txt
```

## ⚙️ Configuração

### 1. Configure o Banco PostgreSQL
Execute o arquivo `banco_de_dados.sql` para criar a estrutura:
```sql
CREATE DATABASE Dados_RFB;
```

### 2. Configure o Arquivo .env
Copie o template e configure suas credenciais:
```bash
cp .env.template .env
```

Edite o arquivo `.env`:
```env
# Configuração do Banco de Dados
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=sua_senha_aqui
DB_NAME=Dados_RFB

# Diretórios
OUTPUT_FILES_PATH=./output
EXTRACTED_FILES_PATH=./files

# Configurações Opcionais
MAX_DOWNLOAD_WORKERS=5
DOWNLOAD_TIMEOUT=1800
```

### 3. Valide a Configuração
O script automaticamente validará suas configurações ao iniciar.

## 🚀 Uso

### Execução Básica
```bash
python ETL_coletar_dados_e_gravar_BD.py
```

### Configuração Avançada
Você pode ajustar o comportamento modificando as variáveis no início do script:

```python
# Configurações de Download
MAX_DOWNLOAD_WORKERS = 5  # 1-10 downloads simultâneos
DOWNLOAD_TIMEOUT = 1800   # Timeout em segundos

# Período dos dados
YEAR = 2025
MONTH = 8
```

### Monitoramento
O script fornece logs detalhados:
- ⏱️ Tempo de execução de cada etapa
- 📊 Estatísticas de download e processamento
- ❌ Relatórios de erro detalhados
- ✅ Confirmação de sucesso

## 🗄️ Estrutura do Banco de Dados

O pipeline cria 10 tabelas estruturadas:

### Tabelas Principais
- **`empresa`**: Dados cadastrais da empresa (matriz)
- **`estabelecimento`**: Dados por unidade/filial (endereços, telefones)
- **`socios`**: Quadro societário completo
- **`simples`**: Dados de MEI e Simples Nacional

### Tabelas de Apoio
- **`cnae`**: Códigos e descrições de atividade
- **`quals`**: Qualificações de pessoas físicas
- **`natju`**: Naturezas jurídicas
- **`moti`**: Motivos de situação cadastral
- **`pais`**: Códigos de países
- **`munic`**: Códigos de municípios

### Índices Automáticos
O sistema cria automaticamente índices otimizados na coluna `cnpj_basico` para todas as tabelas principais.

## ⚡ Performance

### Tempos de Processamento Esperados

| Componente | Hardware Básico | Hardware Recomendado |
|------------|-----------------|---------------------|
| Download | 2-4 horas | 30-60 minutos |
| Extração | 5-10 minutos | 2-3 minutos |
| Carga BD | 4-8 horas | 1-2 horas |
| **Total** | **6-12 horas** | **1.5-3 horas** |

### Dicas de Otimização
- Use SSD para melhor I/O
- Configure PostgreSQL para bulk insert
- Monitore uso de RAM durante execução
- Use conexão de internet estável

## 🔧 Troubleshooting

### Problemas Comuns

#### Erro de Conexão com Banco
```
❌ Erro ao conectar com o banco de dados
```
**Solução**: Verifique arquivo `.env` e se PostgreSQL está rodando.

#### Download Interrompido
```
⚠️ Muitos arquivos falharam no download
```
**Solução**: Reduza `MAX_DOWNLOAD_WORKERS` para 2-3 e tente novamente.

#### Memória Insuficiente
```
❌ Erro de memória durante processamento
```
**Solução**: Feche outras aplicações e monitore uso de RAM.

Este script irá:
- ✅ Testar conectividade básica
- ✅ Verificar se o banco existe (e criar se necessário)
- ✅ Validar credenciais
- ✅ Mostrar versão do PostgreSQL

### Logs e Debug
O sistema gera logs detalhados para facilitar o debug:
- ✅ Operações bem-sucedidas
- ⚠️ Avisos e recuperações
- ❌ Erros com stack trace
- 📊 Estatísticas de performance

## 🏗️ Desenvolvimento

### Estrutura do Projeto
```
Dados-Publicos-Receita-Federal-Brasil/
├── ETL_coletar_dados_e_gravar_BD.py    # Script principal otimizado
├── debug_conexao.py                     # Script de debug de conexão
├── banco_de_dados.sql                   # Schema do banco
├── requirements.txt                     # Dependências Python
├── .env.template                        # Template de configuração
├── README.md                           # Esta documentação
└── output/                             # Arquivos baixados (auto-criado)
└── files/                              # Arquivos extraídos (auto-criado)
```

### Contribuindo
1. Fork este repositório
2. Crie uma branch para sua feature
3. Faça commit das mudanças
4. Abra um Pull Request

## 📝 Changelog

### v2.0.0 (2025) - Versão Otimizada
- ✅ Novo Modelo de Consulta da Receita com paginação Mês e Ano
- ✅ Download paralelo com 5 threads simultâneas
- ✅ Sistema robusto de validação e recuperação
- ✅ Configuração completa via .env
- ✅ Logs visuais com progress tracking
- ✅ Tratamento de erros otimizado
- ✅ Gestão inteligente de memória
- ✅ Documentação completa atualizada
- ✅ Script de debug de conexão
- ✅ Criação automática de banco de dados

### v1.0.0 (Original)
- ✅ Download sequencial básico
- ✅ Processamento e carga no PostgreSQL
- ✅ Estrutura de tabelas fundamental

## 👨‍💻 Créditos

### Referencia
**Aphonso Henrique do Amaral Rafael**
- 🔗 GitHub: [aphonsoar](https://github.com/aphonsoar)
- 📁 Repositório Original: [Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ](https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ)
- 💡 Criador da estrutura fundamental e lógica de processamento

### Melhorias e Otimizações
**Victor Beppler**
- 🔗 GitHub: [victorbeppler](https://github.com/victorbeppler)
- 📁 Este Repositório: [Dados-Publicos---Receita-Federal-Brasil-](https://github.com/victorbeppler/Dados-Publicos---Receita-Federal-Brasil-.git)
- ⚡ Implementação de download paralelo,Reformulação do Modelo de Download com a nova estrutura da Receita contando com as paginações de mês e ano, sistema de configuração .env, tratamento robusto de erros e melhorias de UX

### Reconhecimentos
- 🏛️ **Receita Federal do Brasil** - Pela disponibilização dos dados públicos
- 🐍 **Comunidade Python** - Pelas bibliotecas utilizadas
- 🐘 **PostgreSQL Team** - Pelo excelente SGBD
- 👥 **Comunidade Open Source** - Por feedback e contribuições

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

## 🌟 Como Ajudar

- ⭐ Dê uma estrela no repositório
- 🐛 Reporte bugs e problemas
- 💡 Sugira melhorias
- 🔧 Contribua com código
- 📢 Compartilhe o projeto

---

## 📞 Suporte

Para dúvidas, problemas ou sugestões:

1. 📋 Abra uma [Issue](https://github.com/victorbeppler/Dados-Publicos---Receita-Federal-Brasil-/issues)
2. 💬 Entre em contato via GitHub
3. 📖 Consulte esta documentação
4. 🔍 Verifique o [repositório original](https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ) para referências

---

**🚀 Developed with ❤️ for the Brazilian Open Data Community**

> Este projeto visa democratizar o acesso aos dados públicos de CNPJ, facilitando análises econômicas, acadêmicas e de transparência.