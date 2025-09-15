import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import numpy as np
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# -------------------- CONFIGURAÇÃO AVANÇADA --------------------
SCOPE = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]

# Configuração de cache
CACHE_DURATION = 300  # 5 minutos em segundos

# -------------------- FUNÇÕES OTIMIZADAS --------------------
@st.cache_resource(show_spinner=False)
def get_google_sheets_client():
    """Obtém cliente do Google Sheets com cache para evitar múltiplas autenticações"""
    try:
        # Configuração para Streamlit Cloud (secrets)
        creds_dict = {
            "type": st.secrets["google_service_account"]["type"],
            "project_id": st.secrets["google_service_account"]["project_id"],
            "private_key_id": st.secrets["google_service_account"]["private_key_id"],
            "private_key": st.secrets["google_service_account"]["private_key"],
            "client_email": st.secrets["google_service_account"]["client_email"],
            "client_id": st.secrets["google_service_account"]["client_id"],
            "auth_uri": st.secrets["google_service_account"]["auth_uri"],
            "token_uri": st.secrets["google_service_account"]["token_uri"],
            "auth_provider_x509_cert_url": st.secrets["google_service_account"]["auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["google_service_account"]["client_x509_cert_url"]
        }
        
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Erro na autenticação: {e}")
        return None

@st.cache_data(ttl=CACHE_DURATION, show_spinner="Carregando dados...")
def carregar_todas_abas(_client, sheet_id):
    """Carrega todas as abas de uma vez para minimizar requests à API"""
    try:
        spreadsheet = _client.open_by_key(sheet_id)
        worksheets = spreadsheet.worksheets()
        
        dados = {}
        for worksheet in worksheets:
            try:
                worksheet_data = worksheet.get_all_records()
                df = pd.DataFrame(worksheet_data)
                
                # Converter colunas numéricas para o tipo correto
                if worksheet.title == "atendimentos" and not df.empty:
                    if 'MEDIA_ATENDIMENTO' in df.columns:
                        df['MEDIA_ATENDIMENTO'] = pd.to_numeric(df['MEDIA_ATENDIMENTO'], errors='coerce')
                
                dados[worksheet.title] = df
                
            except Exception as e:
                st.warning(f"Erro ao carregar aba {worksheet.title}: {e}")
                dados[worksheet.title] = pd.DataFrame()
        
        return dados
    except Exception as e:
        st.error(f"Erro ao carregar planilha: {e}")
        return {}

def salvar_dados_otimizado(_client, sheet_id, aba_nome, df):
    """Salva dados de forma otimizada, atualizando apenas quando necessário"""
    try:
        spreadsheet = _client.open_by_key(sheet_id)
        
        try:
            worksheet = spreadsheet.worksheet(aba_nome)
        except:
            worksheet = spreadsheet.add_worksheet(title=aba_nome, rows=1000, cols=20)
        
        # Limpa a aba
        worksheet.clear()
        
        # Adiciona dados de forma eficiente
        if not df.empty:
            # Converte DataFrame para lista de listas
            values = [df.columns.tolist()] + df.values.tolist()
            worksheet.update(values, value_input_option='USER_ENTERED')
        
        # Limpa cache para forçar recarregamento
        st.cache_data.clear()
        return True
        
    except Exception as e:
        st.error(f"Erro ao salvar dados: {e}")
        return False

# -------------------- INICIALIZAÇÃO OTIMIZADA --------------------
def inicializar_sistema():
    """Inicializa o sistema de forma otimizada"""
    client = get_google_sheets_client()
    if not client:
        st.stop()
    
    SHEET_ID = "1VQBd0TR0jlmP04hw8N4HTXnfOqeBmTvSQyRZby1iyb0"
    
    # Carrega todos os dados de uma vez
    todas_abas = carregar_todas_abas(client, SHEET_ID)
    
    # Inicializa abas se não existirem
    if "operacoes" not in todas_abas or todas_abas["operacoes"].empty:
        operacoes = pd.DataFrame({
            'OPERAÇÃO': ['ADVENTURE', 'ACHE', 'BIMBO SP X RJ'],
            'OPERAÇÃO TITULAR': ['ADVENTURE TITULAR', 'ACHE TITULAR', 'BIMBO TITULAR'],
            'MARCA': ['SCANIA', 'VOLVO', 'MERCEDES'],
            'MODELO': ['R500', 'FH540', 'ACTROS'],
            'TIPO': ['LONGO CURSO', 'URBANO', 'REGIONAL'],
            'META': [3.8, 3.5, 3.6],
            'DATA_CRIACAO': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * 3
        })
        salvar_dados_otimizado(client, SHEET_ID, "operacoes", operacoes)
        todas_abas["operacoes"] = operacoes

    if "veiculos" not in todas_abas or todas_abas["veiculos"].empty:
        veiculos = pd.DataFrame({
            'PLACA': ['SWENJYS', 'TEP301', 'FCU033', 'STQ5022'],
            'MARCA': ['SCANIA', 'SCANIA', 'SCANIA', 'SCANIA'],
            'MODELO': ['SCANIA RH 460 A', 'SCANIA RSO NA', 'SCANIA RH 460 A', 'SCANIA RH 460 A'],
            'OPERAÇÃO': ['FIRE - MTIRO', 'FIRE - CONTRAIRER', 'MEU - 1 CONDUTOR', 'FIRE - MTIRO'],
            'MOTORISTA': ['João Silva', 'Maria Santos', 'Pedro Costa', 'Ana Oliveira'],
            'DATA_CADASTRO': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * 4
        })
        salvar_dados_otimizado(client, SHEET_ID, "veiculos", veiculos)
        todas_abas["veiculos"] = veiculos

    if "atendimentos" not in todas_abas or todas_abas["atendimentos"].empty:
        atendimentos = pd.DataFrame({
            'MOTORISTA': ['João Silva', 'Maria Santos'],
            'COLABORADOR': ['Carlos Abordador', 'Ana Fiscal'],
            'DATA_ABORDAGEM': ['2023-09-01', '2023-09-02'],
            'DATA_LANCAMENTO': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * 2,
            'PLACA': ['SWENJYS', 'TEP301'],
            'MODELO': ['SCANIA RH 460 A', 'SCANIA RSO NA'],
            'REVISAO': ['Em dia', 'Atrasada'],
            'TACOGRAFO': ['OK', 'Com problemas'],
            'OPERACAO': ['FIRE - MTIRO', 'FIRE - CONTRAIRER'],
            'DATA_INICIO': ['2023-09-01', '2023-09-02'],
            'DATA_FIM': ['2023-09-10', '2023-09-12'],
            'MEDIA_ATENDIMENTO': [3.40, 2.70],
            'OBSERVACAO': ['Veículo em boas condições', 'Necessita revisão'],
            'DATA_MODIFICACAO': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * 2,
            'MODIFICADO_POR': ['Sistema', 'Sistema']
        })
        salvar_dados_otimizado(client, SHEET_ID, "atendimentos", atendimentos)
        todas_abas["atendimentos"] = atendimentos
    
    return client, SHEET_ID, todas_abas

# -------------------- INTERFACE COMPLETA --------------------
def main():
    st.set_page_config(page_title="Sistema de Abordagens", layout="wide", page_icon="🚛")
    
    # Inicialização otimizada
    with st.spinner("Inicializando sistema..."):
        client, SHEET_ID, todas_abas = inicializar_sistema()
    
    # Acessa dados do cache
    df_operacoes = todas_abas.get("operacoes", pd.DataFrame())
    df_veiculos = todas_abas.get("veiculos", pd.DataFrame())
    df_atendimentos = todas_abas.get("atendimentos", pd.DataFrame())
    
    # Garantir que a coluna MEDIA_ATENDIMENTO seja numérica
    if not df_atendimentos.empty and 'MEDIA_ATENDIMENTO' in df_atendimentos.columns:
        df_atendimentos['MEDIA_ATENDIMENTO'] = pd.to_numeric(df_atendimentos['MEDIA_ATENDIMENTO'], errors='coerce')
    
    # CSS para melhor performance de renderização
    st.markdown("""
    <style>
        .main-header { font-size: 2.5rem; color: #1f77b4; text-align: center; margin-bottom: 2rem; }
        .metric-card { background-color: #f0f2f6; padding: 1rem; border-radius: 0.5rem; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); }
        .stButton>button { width: 100%; background-color: #1f77b4; color: white; }
        [data-testid="stForm"] { border: none; }
        .stDataFrame { font-size: 0.9rem; }
    </style>
    """, unsafe_allow_html=True)
    
    # Menu lateral simplificado
    with st.sidebar:
        st.title("🚛 Sistema de Abordagens")
        menu = st.radio("Navegação", [
            "Dashboard", "Operações", "Registros", "Histórico", "Veículos"
        ], key="menu_navigation")
        
        # Informações do usuário (simulado)
        st.sidebar.markdown("---")
        usuario = st.sidebar.text_input("Seu nome", value="Fiscal", key="user_name")
        
        if st.button("🔄 Atualizar Dados", use_container_width=True, key="refresh_button"):
            st.cache_data.clear()
            st.rerun()
        
        st.info("💡 Dados atualizados a cada 5 minutos")
    
    # ----------------------- DASHBOARD -----------------------
    if menu == "Dashboard":
        st.markdown('<h1 class="main-header">Dashboard de Abordagens</h1>', unsafe_allow_html=True)
        
        # Adicionar OPERAÇÃO TITULAR aos dados de atendimento
        if not df_atendimentos.empty and not df_operacoes.empty:
            # Criar mapeamento de OPERACAO para OPERAÇÃO TITULAR
            operacao_titular_map = df_operacoes.set_index('OPERAÇÃO')['OPERAÇÃO TITULAR'].to_dict()
            df_atendimentos['OPERAÇÃO TITULAR'] = df_atendimentos['OPERACAO'].map(operacao_titular_map)
        
        # Métricas com cache
        col1, col2, col3, col4 = st.columns(4)
        with col1: st.metric("Total de Veículos", len(df_veiculos))
        with col2: st.metric("Total de Atendimentos", len(df_atendimentos))
        with col3: st.metric("Operações Cadastradas", len(df_operacoes))
        with col4: 
            # Calcular média com tratamento de erros
            if not df_atendimentos.empty and 'MEDIA_ATENDIMENTO' in df_atendimentos.columns:
                try:
                    media_geral = df_atendimentos['MEDIA_ATENDIMENTO'].mean()
                    if pd.isna(media_geral):
                        media_geral = 0
                except:
                    media_geral = 0
            else:
                media_geral = 0
            st.metric("Média Geral", f"{media_geral:.2f}")
        
        # Gráficos otimizados
        if not df_atendimentos.empty and 'OPERAÇÃO TITULAR' in df_atendimentos.columns:
            col1, col2 = st.columns(2)
            with col1:
                operacao_count = df_atendimentos['OPERAÇÃO TITULAR'].value_counts()
                fig = px.pie(values=operacao_count.values, names=operacao_count.index, title="Atendimentos por Operação Titular")
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                revisao_count = df_atendimentos['REVISAO'].value_counts()
                fig = px.bar(x=revisao_count.index, y=revisao_count.values, title="Status de Revisão")
                st.plotly_chart(fig, use_container_width=True)
        
        # Últimos registros
        st.subheader("Últimos Atendimentos")
        if not df_atendimentos.empty:
            st.dataframe(df_atendimentos.tail(5)[['PLACA', 'DATA_ABORDAGEM', 'OPERACAO', 'MEDIA_ATENDIMENTO']],
                        use_container_width=True)
        else:
            st.info("Nenhum atendimento registrado ainda.")

    # ----------------------- OPERAÇÕES -----------------------
    elif menu == "Operações":
        st.markdown('<h1 class="main-header">Operações e Metas</h1>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("Adicionar Nova Operação")
            
            with st.form("nova_operacao", clear_on_submit=True):
                operacao = st.text_input("OPERAÇÃO", key="operacao_input")
                operacao_titular = st.text_input("OPERAÇÃO TITULAR", key="operacao_titular_input")
                marca = st.text_input("MARCA", key="marca_operacao_input")
                modelo = st.text_input("MODELO", key="modelo_operacao_input")
                tipo = st.text_input("TIPO", key="tipo_operacao_input")
                meta = st.number_input("META", min_value=0.0, format="%.2f", key="meta_input")
                
                submitted = st.form_submit_button("Adicionar Operação")
                
                if submitted:
                    nova_operacao = pd.DataFrame({
                        'OPERAÇÃO': [operacao],
                        'OPERAÇÃO TITULAR': [operacao_titular],
                        'MARCA': [marca],
                        'MODELO': [modelo],
                        'TIPO': [tipo],
                        'META': [meta],
                        'DATA_CRIACAO': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
                    })
                    
                    df_operacoes = pd.concat([df_operacoes, nova_operacao], ignore_index=True)
                    if salvar_dados_otimizado(client, SHEET_ID, "operacoes", df_operacoes):
                        st.success("Operação adicionada com sucesso!")
                        time.sleep(1)
                        st.rerun()
        
        with col2:
            st.subheader("Operações Cadastradas")
            if not df_operacoes.empty:
                st.dataframe(df_operacoes, use_container_width=True)
            else:
                st.info("Nenhuma operação cadastrada ainda.")

    # ----------------------- REGISTROS -----------------------
    elif menu == "Registros":
        st.markdown('<h1 class="main-header">Registro de Atendimentos</h1>', unsafe_allow_html=True)
        
        # Primeiro, vamos criar todas as variáveis fora do formulário
        placa_selecionada = None
        veiculo_info = None
        
        # Obter informações do veículo antes do formulário
        if not df_veiculos.empty:
            placas = df_veiculos["PLACA"].unique()
            placa_selecionada = st.selectbox("PLACA", options=placas, key="placa_select")
            
            # Preencher automaticamente dados do veículo
            veiculo_info = df_veiculos[df_veiculos["PLACA"] == placa_selecionada].iloc[0] if placa_selecionada else None
        
        with st.form("registro_atendimento", clear_on_submit=True):
            col1, col2 = st.columns(2)
            
            with col1:
                # Exibir informações do veículo (apenas leitura)
                if veiculo_info is not None:
                    st.text_input("MARCA", value=veiculo_info.get("MARCA", ""), disabled=True, key="marca_display")
                    st.text_input("MODELO", value=veiculo_info.get("MODELO", ""), disabled=True, key="modelo_display")
                    st.text_input("MOTORISTA", value=veiculo_info.get("MOTORISTA", ""), disabled=True, key="motorista_display")
                    st.text_input("OPERAÇÃO", value=veiculo_info.get("OPERAÇÃO", ""), disabled=True, key="operacao_display")
                
                data_abordagem = st.date_input("DATA DE ABORDAGEM", value=datetime.today(), key="data_abordagem")
                revisao = st.selectbox("REVISÃO", options=["Em dia", "Atrasada", "Não se aplica"], key="revisao_select")
                tacografo = st.selectbox("TACÓGRAFO", options=["OK", "Com problemas", "Não verificado"], key="tacografo_select")
                
                # Dados de operação
                if not df_operacoes.empty:
                    operacoes = df_operacoes["OPERAÇÃO"].unique()
                    operacao_abordagem = st.selectbox("OPERAÇÃO", options=operacoes, key="operacao_abordagem_select")
            
            with col2:
                data_inicio = st.date_input("DATA INÍCIO", value=datetime.today(), key="data_inicio")
                data_fim = st.date_input("DATA FIM", value=datetime.today() + timedelta(days=7), key="data_fim")
                media_atendimento = st.number_input("MÉDIA ATENDIMENTO", min_value=0.0, format="%.2f", key="media_atendimento")
                
                # Informações do colaborador
                motorista_valor = veiculo_info.get("MOTORISTA", "") if veiculo_info is not None else ""
                motorista = st.text_input("MOTORISTA", value=motorista_valor, key="motorista_input")
                colaborador = st.text_input("COLABORADOR", value=usuario, key="colaborador_input")
                observacao = st.text_area("OBSERVAÇÃO", key="observacao_text")
            
            enviar = st.form_submit_button("Enviar Atendimento")
            
            if enviar:
                novo_atendimento = pd.DataFrame({
                    "MOTORISTA": [motorista],
                    "COLABORADOR": [colaborador],
                    "DATA_ABORDAGEM": [data_abordagem.strftime("%Y-%m-%d")],
                    "DATA_LANCAMENTO": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                    "PLACA": [placa_selecionada],
                    "MODELO": [veiculo_info.get("MODELO", "") if veiculo_info is not None else ""],
                    "REVISAO": [revisao],
                    "TACOGRAFO": [tacografo],
                    "OPERACAO": [operacao_abordagem],
                    "DATA_INICIO": [data_inicio.strftime("%Y-%m-%d")],
                    "DATA_FIM": [data_fim.strftime("%Y-%m-%d")],
                    "MEDIA_ATENDIMENTO": [media_atendimento],
                    "OBSERVACAO": [observacao],
                    "DATA_MODIFICACAO": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                    "MODIFICADO_POR": [usuario]
                })
                
                df_atendimentos = pd.concat([df_atendimentos, novo_atendimento], ignore_index=True)
                if salvar_dados_otimizado(client, SHEET_ID, "atendimentos", df_atendimentos):
                    st.success("Atendimento registrado com sucesso!")
                    time.sleep(1)
                    st.rerun()

    # ----------------------- HISTÓRICO -----------------------
    elif menu == "Histórico":
        st.markdown('<h1 class="main-header">Histórico de Atendimentos</h1>', unsafe_allow_html=True)
        
        # Adicionar OPERAÇÃO TITULAR aos dados de atendimento
        if not df_atendimentos.empty and not df_operacoes.empty:
            # Criar mapeamento de OPERACAO para OPERAÇÃO TITULAR
            operacao_titular_map = df_operacoes.set_index('OPERAÇÃO')['OPERAÇÃO TITULAR'].to_dict()
            df_atendimentos['OPERAÇÃO TITULAR'] = df_atendimentos['OPERACAO'].map(operacao_titular_map)
        
        # Filtros
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if not df_atendimentos.empty and 'DATA_ABORDAGEM' in df_atendimentos.columns:
                # Corrigir o problema de formato de data
                try:
                    # Tentar converter para datetime com formato específico
                    df_atendimentos["DATA_ABORDAGEM"] = pd.to_datetime(
                        df_atendimentos["DATA_ABORDAGEM"], 
                        format='mixed', 
                        dayfirst=True,
                        errors='coerce'
                    )
                    
                    # Filtrar apenas as datas válidas
                    datas_validas = df_atendimentos[df_atendimentos["DATA_ABORDAGEM"].notna()]
                    if not datas_validas.empty:
                        datas_disponiveis = datas_validas["DATA_ABORDAGEM"].dt.date.unique()
                        data_inicio = st.date_input("Data Início", value=min(datas_disponiveis), key="filtro_data_inicio")
                        data_fim = st.date_input("Data Fim", value=max(datas_disponiveis), key="filtro_data_fim")
                    else:
                        data_inicio = st.date_input("Data Início", value=datetime.today(), key="filtro_data_inicio_default")
                        data_fim = st.date_input("Data Fim", value=datetime.today(), key="filtro_data_fim_default")
                except:
                    data_inicio = st.date_input("Data Início", value=datetime.today(), key="filtro_data_inicio_default")
                    data_fim = st.date_input("Data Fim", value=datetime.today(), key="filtro_data_fim_default")
        
        with col2:
            if not df_atendimentos.empty and 'OPERAÇÃO TITULAR' in df_atendimentos.columns:
                operacoes = df_atendimentos["OPERAÇÃO TITULAR"].unique()
                operacao_filtro = st.multiselect("Operação Titular", options=operacoes, key="filtro_operacao")
        
        with col3:
            if not df_atendimentos.empty and 'PLACA' in df_atendimentos.columns:
                placas = df_atendimentos["PLACA"].unique()
                placa_filtro = st.multiselect("Placa", options=placas, key="filtro_placa")
        
        # Aplicar filtros
        if not df_atendimentos.empty:
            df_filtrado = df_atendimentos.copy()
            
            # Aplicar filtro de data se disponível
            if 'data_inicio' in locals() and 'data_fim' in locals() and 'DATA_ABORDAGEM' in df_filtrado.columns:
                try:
                    # Garantir que a coluna DATA_ABORDAGEM seja datetime
                    df_filtrado["DATA_ABORDAGEM"] = pd.to_datetime(
                        df_filtrado["DATA_ABORDAGEM"], 
                        format='mixed', 
                        dayfirst=True,
                        errors='coerce'
                    )
                    
                    # Filtrar por data
                    df_filtrado = df_filtrado[
                        (df_filtrado["DATA_ABORDAGEM"].dt.date >= data_inicio) & 
                        (df_filtrado["DATA_ABORDAGEM"].dt.date <= data_fim)
                    ]
                except:
                    st.warning("Erro ao filtrar por data. Mostrando todos os registros.")
            
            # Aplicar filtro de operação
            if 'operacao_filtro' in locals() and operacao_filtro and 'OPERAÇÃO TITULAR' in df_filtrado.columns:
                df_filtrado = df_filtrado[df_filtrado["OPERAÇÃO TITULAR"].isin(operacao_filtro)]
            
            # Aplicar filtro de placa
            if 'placa_filtro' in locals() and placa_filtro and 'PLACA' in df_filtrado.columns:
                df_filtrado = df_filtrado[df_filtrado["PLACA"].isin(placa_filtro)]
            
            # Estatísticas com tratamento de erros
            total_atendimentos = len(df_filtrado)
            placas_unicas = df_filtrado["PLACA"].nunique() if 'PLACA' in df_filtrado.columns else 0
            
            # Calcular média com tratamento de erros
            if 'MEDIA_ATENDIMENTO' in df_filtrado.columns:
                try:
                    media_geral = df_filtrado['MEDIA_ATENDIMENTO'].mean()
                    if pd.isna(media_geral):
                        media_geral = 0
                except:
                    media_geral = 0
            else:
                media_geral = 0
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Total de Atendimentos", total_atendimentos)
            col2.metric("Veículos Únicos", placas_unicas)
            col3.metric("Média Geral", f"{media_geral:.2f}")
            
            # Gráfico de atendimentos por operação titular
            st.subheader("Atendimentos por Operação Titular")
            if not df_filtrado.empty and 'OPERAÇÃO TITULAR' in df_filtrado.columns:
                atendimentos_por_operacao = df_filtrado["OPERAÇÃO TITULAR"].value_counts().reset_index()
                atendimentos_por_operacao.columns = ["Operação Titular", "Quantidade"]
                
                fig = px.bar(
                    atendimentos_por_operacao, 
                    x="Operação Titular", 
                    y="Quantidade",
                    title="Quantidade de Atendimentos por Operação Titular"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Nenhum dado para exibir com os filtros selecionados.")
            
            # Tabela de dados
            st.subheader("Dados Detalhados")
            st.dataframe(df_filtrado, use_container_width=True)
        else:
            st.info("Nenhum atendimento registrado ainda.")

    # ----------------------- VEÍCULOS -----------------------
    elif menu == "Veículos":
        st.markdown('<h1 class="main-header">Gestão de Veículos</h1>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("Adicionar Novo Veículo")
            
            with st.form("novo_veiculo", clear_on_submit=True):
                placa = st.text_input("PLACA", key="veiculo_placa")
                marca = st.text_input("MARCA", key="veiculo_marca")
                modelo = st.text_input("MODELO", key="veiculo_modelo")
                operacao = st.text_input("OPERAÇÃO", key="veiculo_operacao")
                motorista = st.text_input("MOTORISTA", key="veiculo_motorista")
                
                submitted = st.form_submit_button("Adicionar Veículo")
                
                if submitted:
                    novo_veiculo = pd.DataFrame({
                        'PLACA': [placa],
                        'MARCA': [marca],
                        'MODELO': [modelo],
                        'OPERAÇÃO': [operacao],
                        'MOTORISTA': [motorista],
                        'DATA_CADASTRO': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
                    })
                    
                    df_veiculos = pd.concat([df_veiculos, novo_veiculo], ignore_index=True)
                    if salvar_dados_otimizado(client, SHEET_ID, "veiculos", df_veiculos):
                        st.success("Veículo adicionado com sucesso!")
                        time.sleep(1)
                        st.rerun()
        
        with col2:
            st.subheader("Veículos Cadastrados")
            if not df_veiculos.empty:
                st.dataframe(df_veiculos, use_container_width=True)
            else:
                st.info("Nenhum veículo cadastrado ainda.")

if __name__ == "__main__":
    main()