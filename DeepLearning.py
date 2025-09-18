import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import numpy as np
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from streamlit.components.v1 import html
import json
from io import BytesIO
import base64

# -------------------- CONFIGURAÇÃO AVANÇADA --------------------
SCOPE = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive",
         "https://www.googleapis.com/auth/spreadsheets"]

# Configuração de cache otimizada
CACHE_DURATION = 180  # 3 minutos em segundos
SENHA_ADMIN = "Telemetria@2025"  # Senha para modificar operações e veículos

# Dados de usuários para autenticação
USUARIOS = {
    "lucas.alves@transmaroni.com.br": {
        "senha": "Maroni@25",
        "nome": "Lucas Roberto de Sousa Alves"
    },
    "amanda.soares@transmaroni.com.br": {
        "senha": "Maroni@25",
        "nome": "Amanda Lima Soares"
    },
    "james.rosario@transmaroni.com.br": {
        "senha": "Maroni@25",
        "nome": "James Marques Do Rosario"
    },
    "henrique.araujo@transmaroni.com.br": {
        "senha": "Maroni@25",
        "nome": "Henrique Torres Araujo"
    },
    "amanda.carvalho@transmaroni.com.br": {
        "senha": "Maroni@25",
        "nome": "Amanda Stefane Santos Carvalho"
    },
    "giovanna.oliveira@transmaroni.com.br": {
        "senha": "Maroni@25",
        "nome": "Giovanna Assunção de Oliveira"
    }
}

# -------------------- FUNÇÕES SUPER OTIMIZADAS --------------------
@st.cache_resource(show_spinner=False, ttl=3600)
def get_google_sheets_client():
    """Obtém cliente do Google Sheets com cache prolongado"""
    try:
        creds_dict = {
            "type": st.secrets["google_service_account"]["type"],
            "project_id": st.secrets["google_service_account"]["project_id"],
            "private_key_id": st.secrets["google_service_account"]["private_key_id"],
            "private_key": st.secrets["google_service_account"]["private_key"].replace('\\n', '\n'),
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
        st.error(f"Erro na autenticação: {str(e)}")
        return None

@st.cache_data(ttl=CACHE_DURATION, show_spinner="📊 Carregando dados...")
def carregar_dados_otimizado(_client, sheet_id):
    """Carrega dados de forma otimizada com tratamento de erros"""
    try:
        spreadsheet = _client.open_by_key(sheet_id)
        dados = {}
        
        # Mapeamento de abas para carregar
        abas_necessarias = ["operacoes", "veiculos", "atendimentos"]
        
        for aba_nome in abas_necessarias:
            try:
                worksheet = spreadsheet.worksheet(aba_nome)
                records = worksheet.get_all_records()
                df = pd.DataFrame(records)
                
                # Conversões otimizadas de tipos de dados
                if not df.empty:
                    # Converter colunas de data
                    date_columns = ['DATA_ABORDAGEM', 'DATA_LANCAMENTO', 'DATA_INICIO', 'DATA_FIM', 'DATA_MODIFICACAO', 'DATA_CRIACAO', 'DATA_CADASTRO']
                    for col in date_columns:
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                    
                    # Converter colunas numéricas
                    numeric_columns = ['META', 'MEDIA_ATENDIMENTO']
                    for col in numeric_columns:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                
                dados[aba_nome] = df
                
            except Exception as e:
                st.warning(f"Aba {aba_nome} não encontrada ou vazia: {str(e)}")
                dados[aba_nome] = pd.DataFrame()
        
        return dados
        
    except Exception as e:
        st.error(f"Erro ao carregar planilha: {str(e)}")
        return {}

def converter_datetime_para_string(obj):
    """Função auxiliar para converter datetime para string durante a serialização"""
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.strftime('%d/%m/%Y %H:%M:%S')
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def salvar_dados_eficiente(_client, sheet_id, aba_nome, df):
    """Salva dados de forma eficiente com batch processing"""
    try:
        spreadsheet = _client.open_by_key(sheet_id)
        
        try:
            worksheet = spreadsheet.worksheet(aba_nome)
        except:
            worksheet = spreadsheet.add_worksheet(title=aba_nome, rows=1000, cols=20)
        
        # Prepara dados para upload
        if not df.empty:
            # Converte todas as colunas de datetime para string
            df = df.copy()
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.strftime('%d/%m/%Y %H:%M:%S')
                # Converte outros tipos de dados problemáticos
                elif pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(0)
            
            # Garante que todos os valores sejam strings ou números
            values = [df.columns.tolist()] + df.astype(str).values.tolist()
            
            worksheet.clear()
            worksheet.update(values, value_input_option='USER_ENTERED')
        
        # Limpa cache de forma seletiva
        st.cache_data.clear()
        return True
        
    except Exception as e:
        st.error(f"Erro ao salvar dados: {str(e)}")
        return False

# -------------------- INICIALIZAÇÃO RÁPIDA --------------------
def inicializar_sistema():
    """Inicializa o sistema de forma ultra rápida"""
    client = get_google_sheets_client()
    if not client:
        st.stop()
    
    SHEET_ID = "1VQBd0TR0jlmP04hw8N4HTXnfOqeBmTvSQyRZby1iyb0"
    
    # Carrega dados com loading otimizado
    with st.spinner("⚡ Carregando dados..."):
        todas_abas = carregar_dados_otimizado(client, SHEET_ID)
    
    return client, SHEET_ID, todas_abas

# -------------------- COMPONENTES DE UI AVANÇADOS --------------------
def criar_metric_card(title, value, icon="📊", delta=None):
    """Cria um card de métrica estilizado"""
    card_html = f"""
    <div style="background: linear-gradient(135deg, #FFD700 0%, #FFA500 100%); 
                padding: 1.5rem; 
                border-radius: 12px; 
                color: white; 
                text-align: center;
                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                margin: 0.5rem;">
        <div style="font-size: 2rem; margin-bottom: 0.5rem;">{icon}</div>
        <div style="font-size: 1.2rem; font-weight: bold; margin-bottom: 0.5rem;">{title}</div>
        <div style="font-size: 2rem; font-weight: bold;">{value}</div>
        {f'<div style="font-size: 1rem; margin-top: 0.5rem;">{delta}</div>' if delta else ''}
    </div>
    """
    return html(card_html, height=200)

def criar_filtros_avancados(df_atendimentos, df_operacoes):
    """Cria interface de filtros avançados"""
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Filtro de data
        if not df_atendimentos.empty and 'DATA_ABORDAGEM' in df_atendimentos.columns:
            datas_validas = df_atendimentos[df_atendimentos['DATA_ABORDAGEM'].notna()]
            if not datas_validas.empty:
                min_date = datas_validas['DATA_ABORDAGEM'].min().date()
                max_date = datas_validas['DATA_ABORDAGEM'].max().date()
                data_range = st.date_input(
                    "📅 Período",
                    value=(),
                    min_value=min_date,
                    max_value=max_date,
                    key="filtro_data"
                )
    
    with col2:
        # Filtro de operação titular
        if not df_atendimentos.empty and 'OPERACAO' in df_atendimentos.columns and not df_operacoes.empty:
            # Criar mapeamento de operação para operação titular
            operacao_titular_map = df_operacoes.set_index('OPERAÇÃO')['OPERAÇÃO TITULAR'].to_dict()
            df_atendimentos['OPERAÇÃO TITULAR'] = df_atendimentos['OPERACAO'].map(operacao_titular_map)
            
            operacoes_titulares = sorted(df_atendimentos['OPERAÇÃO TITULAR'].dropna().unique())
            operacao_filtro = st.multiselect(
                "👑 Operação Titular",
                options=operacoes_titulares,
                default=[],
                key="filtro_operacao_titular"
            )
    
    with col3:
        # Filtro de status de revisão
        if not df_atendimentos.empty and 'REVISAO' in df_atendimentos.columns:
            status_options = sorted(df_atendimentos['REVISAO'].unique())
            status_filtro = st.multiselect(
                "🔧 Status Revisão",
                options=status_options,
                default=[],
                key="filtro_status"
            )
    
    return {
        'data_range': data_range if 'data_range' in locals() else None,
        'operacao_filtro': operacao_filtro,
        'status_filtro': status_filtro
    }

# -------------------- SISTEMA DE AUTENTICAÇÃO --------------------
def autenticar_usuario():
    """Sistema de autenticação de usuários"""
    if 'autenticado' not in st.session_state:
        st.session_state.autenticado = False
        st.session_state.usuario = None
        st.session_state.nome_usuario = None
    
    if not st.session_state.autenticado:
        st.title("🔐 Sistema de Abordagens - Login")
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            with st.form("login_form"):
                st.subheader("Acesso ao Sistema")
                email = st.text_input("📧 E-mail", placeholder="seu.email@transmaroni.com.br")
                senha = st.text_input("🔒 Senha", type="password", placeholder="Sua senha")
                
                submitted = st.form_submit_button("🚀 Entrar no Sistema")
                
                if submitted:
                    if email in USUARIOS and USUARIOS[email]["senha"] == senha:
                        st.session_state.autenticado = True
                        st.session_state.usuario = email
                        st.session_state.nome_usuario = USUARIOS[email]["nome"]
                        st.success(f"✅ Bem-vindo(a), {USUARIOS[email]['nome']}!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("❌ E-mail ou senha incorretos. Tente novamente.")
            
            st.info("💡 Use seu e-mail corporativo e senha fornecidos pela empresa.")
        
        st.stop()
    
    return st.session_state.nome_usuario

# -------------------- INTERFACE PRINCIPAL --------------------
def main():
    st.set_page_config(
        page_title="Sistema de Abordagens - Bomba",
        layout="wide", 
        page_icon="🚛",
        initial_sidebar_state="expanded"
    )
    
    # Autenticar usuário
    nome_usuario = autenticar_usuario()
    
    # CSS Avançado para melhor UX - Tema amarelo mais intenso
    st.markdown("""
    <style>
        .main-header { 
            font-size: 2.5rem; 
            color: white; 
            text-align: left; 
            margin-bottom: 1rem;
            background: linear-gradient(135deg, #FFD700 0%, #FFA500 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: bold;
            padding: 1rem;
            border-radius: 10px;
            margin-left: -2rem;
            margin-top: -2rem;
        }
        .header-container {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1rem;
        }
        .logo-img {
            height: 80px;
            margin-right: -2rem;
            margin-top: -2rem;
        }
        .stButton>button {
            background: linear-gradient(135deg, #FFD700 0%, #FFA500 100%);
            color: white;
            border: none;
            border-radius: 8px;
            padding: 0.5rem 1rem;
            font-weight: bold;
            transition: all 0.3s ease;
        }
        .stButton>button:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(255, 215, 0, 0.3);
        }
        .metric-card {
            background: linear-gradient(135deg, #FFD700 0%, #FFA500 100%);
            padding: 1.5rem;
            border-radius: 12px;
            color: white;
            text-align: center;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        }
        .sidebar .sidebar-content {
            background: linear-gradient(180deg, #FFD700 0%, #FFA500 100%);
            color: white;
        }
        .placa-validada {
            border: 2px solid #28a745 !important;
            background-color: #f8fff9 !important;
        }
        .placa-invalida {
            border: 2px solid #dc3545 !important;
            background-color: #fff5f5 !important;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
        }
        .stTabs [data-baseweb="tab"] {
            background: linear-gradient(135deg, #FFD700 0%, #FFA500 100%);
            color: white;
            border-radius: 8px 8px 0px 0px;
            padding: 10px 16px;
        }
        .stTabs [aria-selected="true"] {
            background: linear-gradient(135deg, #FFA500 0%, #FF8C00 100%) !important;
        }
        .card-indicador {
            background: linear-gradient(135deg, #FFD700 0%, #FFA500 100%);
            padding: 1rem;
            border-radius: 10px;
            color: white;
            text-align: center;
            margin: 0.5rem;
            box-shadow: 0 4px 10px rgba(0,0,0,0.1);
        }
        .table-operacoes {
            max-height: 300px;
            overflow-y: auto;
            border: 1px solid #FFD700;
            border-radius: 8px;
            padding: 10px;
            margin-bottom: 1rem;
        }
        .selected-operation {
            background-color: #FFD700 !important;
            color: white !important;
            font-weight: bold;
        }
        .btn-excluir {
            background: linear-gradient(135deg, #dc3545 0%, #c82333 100%) !important;
            margin-left: 0.5rem;
        }
        .btn-editar {
            background: linear-gradient(135deg, #28a745 0%, #218838 100%) !important;
        }
        .info-oculta {
            display: none !important;
        }
        .meta-atingida {
            background-color: #d4edda !important;
            color: #155724 !important;
            font-weight: bold;
        }
        .meta-nao-atingida {
            background-color: #f8d7da !important;
            color: #721c24 !important;
            font-weight: bold;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Inicialização rápida
    client, SHEET_ID, todas_abas = inicializar_sistema()
    
    # Acessa dados
    df_operacoes = todas_abas.get("operacoes", pd.DataFrame())
    df_veiculos = todas_abas.get("veiculos", pd.DataFrame())
    df_atendimentos = todas_abas.get("atendimentos", pd.DataFrame())
    
    # Menu lateral moderno
    with st.sidebar:
        st.markdown(f"""
        <div style="text-align: center; padding: 1rem;">
            <h1 style="color: white; margin-bottom: 1rem;">🚛 Sistema de Abordagens</h1>
            <p style="color: white; margin-bottom: 1rem;">👤 {nome_usuario}</p>
        </div>
        """, unsafe_allow_html=True)
        
        menu = st.radio("Navegação", [
            "📊 Dashboard", "🏢 Operações", "📝 Registros", 
            "📋 Histórico", "🚗 Veículos"
        ], key="menu_navigation")
        
        st.sidebar.markdown("---")
        
        if st.button("🔄 Atualizar Dados", use_container_width=True, key="refresh_button"):
            st.cache_data.clear()
            st.rerun()
        
        if st.button("🚪 Sair", use_container_width=True, key="logout_button"):
            st.session_state.autenticado = False
            st.session_state.usuario = None
            st.session_state.nome_usuario = None
            st.rerun()
        
        st.info("💡 Dados atualizados a cada 3 minutos")
    
    # ----------------------- DASHBOARD -----------------------
    if "📊 Dashboard" in menu:
        # Header com título e logo
        col_title, col_logo = st.columns([3, 1])
        with col_title:
            st.markdown('<h1 class="main-header">Gestão de Abordados Telemetria</h1>', unsafe_allow_html=True)
        with col_logo:
            # Imagem no canto superior direito
            st.markdown("""
            <div style="text-align: right;">
                <img src="https://cdn-icons-png.flaticon.com/512/1006/1006555.png" class="logo-img" alt="Logo">
            </div>
            """, unsafe_allow_html=True)
        
        # Adicionar OPERAÇÃO TITULAR aos dados de atendimento
        if not df_atendimentos.empty and not df_operacoes.empty:
            operacao_titular_map = df_operacoes.set_index('OPERAÇÃO')['OPERAÇÃO TITULAR'].to_dict()
            df_atendimentos['OPERAÇÃO TITULAR'] = df_atendimentos['OPERACAO'].map(operacao_titular_map)
        
        # Lançamentos do dia
        st.subheader("📈 Lançamentos do Dia")
        hoje = datetime.now().date()
        lancamentos_hoje = 0
        if not df_atendimentos.empty and 'DATA_LANCAMENTO' in df_atendimentos.columns:
            df_atendimentos['DATA_LANCAMENTO'] = pd.to_datetime(df_atendimentos['DATA_LANCAMENTO'], errors='coerce')
            lancamentos_hoje = len(df_atendimentos[df_atendimentos['DATA_LANCAMENTO'].dt.date == hoje])
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("🚗 Total de Veículos", len(df_veiculos), help="Veículos cadastrados no sistema")
        
        with col2:
            st.metric("📋 Total de Atendimentos", len(df_atendimentos), help="Total de abordagens realizadas")
        
        with col3:
            st.metric("🏢 Operações Ativas", len(df_operacoes), help="Operações cadastradas")
        
        with col4:
            if not df_atendimentos.empty and 'MEDIA_ATENDIMENTO' in df_atendimentos.columns:
                media_geral = df_atendimentos['MEDIA_ATENDIMENTO'].mean()
                media_formatada = f"{media_geral:.2f}" if not pd.isna(media_geral) else "0.00"
                st.metric("⭐ Média Geral", media_formatada, help="Média geral de atendimentos")
            else:
                st.metric("⭐ Média Geral", "0.00")
        
        with col5:
            st.metric("📅 Lançamentos Hoje", lancamentos_hoje, help="Atendimentos registrados hoje")
        
        # Gráficos otimizados - usando OPERAÇÃO TITULAR
        if not df_atendimentos.empty and 'OPERAÇÃO TITULAR' in df_atendimentos.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                # Gráfico de pizza - Atendimentos por operação titular
                operacao_count = df_atendimentos['OPERAÇÃO TITULAR'].value_counts().head(10)
                fig = px.pie(
                    values=operacao_count.values, 
                    names=operacao_count.index, 
                    title="📊 Atendimentos por Operação Titular",
                    color_discrete_sequence=px.colors.sequential.YlOrRd
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Gráfico de barras - Quantidade de atendimentos por operação titular
                operacao_count_bar = df_atendimentos['OPERAÇÃO TITULAR'].value_counts().head(10)
                fig_bar = px.bar(
                    x=operacao_count_bar.index,
                    y=operacao_count_bar.values,
                    title="📈 Quantidade de Atendimentos por Operação Titular",
                    labels={'x': 'Operação Titular', 'y': 'Quantidade de Atendimentos'},
                    color=operacao_count_bar.values,
                    color_continuous_scale="ylorrd"
                )
                fig_bar.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_bar, use_container_width=True)
            
            # Gráfico de média por operação titular
            st.subheader("📈 Média de Atendimento por Operação Titular")
            media_por_operacao = df_atendimentos.groupby('OPERAÇÃO TITULAR')['MEDIA_ATENDIMENTO'].mean().reset_index()
            media_por_operacao['MEDIA_ATENDIMENTO'] = media_por_operacao['MEDIA_ATENDIMENTO'].round(2)
            
            fig = px.bar(
                media_por_operacao, 
                x='OPERAÇÃO TITULAR', 
                y='MEDIA_ATENDIMENTO',
                title="Média de Atendimento por Operação Titular",
                color='MEDIA_ATENDIMENTO',
                color_continuous_scale="ylorrd"
            )
            fig.update_layout(yaxis_tickformat=".2f", xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        
        # Gráfico de registros por colaborador
        if not df_atendimentos.empty and 'COLABORADOR' in df_atendimentos.columns:
            st.subheader("📊 Registros por Colaborador")
            colaborador_count = df_atendimentos['COLABORADOR'].value_counts()
            
            fig_colab = px.bar(
                x=colaborador_count.index,
                y=colaborador_count.values,
                title="Quantidade de Registros por Colaborador",
                labels={'x': 'Colaborador', 'y': 'Quantidade de Registros'},
                color=colaborador_count.values,
                color_continuous_scale="ylorrd"
            )
            fig_colab.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_colab, use_container_width=True)
        
        # Últimos registros
        st.subheader("📋 Últimos Atendimentos")
        if not df_atendimentos.empty:
            ultimos_atendimentos = df_atendimentos.tail(5).copy()
            if 'MEDIA_ATENDIMENTO' in ultimos_atendimentos.columns:
                ultimos_atendimentos['MEDIA_ATENDIMENTO'] = ultimos_atendimentos['MEDIA_ATENDIMENTO'].round(2)
            
            st.dataframe(ultimos_atendimentos[[
                'PLACA', 'MOTORISTA', 'DATA_ABORDAGEM', 'OPERACAO', 'MEDIA_ATENDIMENTO'
            ]], use_container_width=True)
        else:
            st.info("Nenhum atendimento registrado ainda.")

    # ----------------------- OPERAÇÕES -----------------------
    elif "🏢 Operações" in menu:
        st.markdown('<h1 class="main-header">Gestão de Operações</h1>', unsafe_allow_html=True)
        
        # Verificação de senha para modificações
        senha = st.text_input("🔒 Senha de Administração", type="password", key="senha_operacoes")
        acesso_permitido = senha == SENHA_ADMIN
        
        if not acesso_permitido and senha:
            st.error("❌ Senha incorreta. Acesso não autorizado.")
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("➕ Adicionar Nova Operação")
            
            with st.form("nova_operacao", clear_on_submit=True):
                operacao = st.text_input("🏢 OPERAÇÃO", key="operacao_input", disabled=not acesso_permitido)
                operacao_titular = st.text_input("👑 OPERAÇÃO TITULAR", key="operacao_titular_input", disabled=not acesso_permitido)
                marca = st.text_input("🏭 MARCA", key="marca_operacao_input", disabled=not acesso_permitido)
                modelo = st.text_input("🔧 MODELO", key="modelo_operacao_input", disabled=not acesso_permitido)
                tipo = st.text_input("📋 TIPO", key="tipo_operacao_input", disabled=not acesso_permitido)
                meta = st.number_input("🎯 META", min_value=0.0, format="%.2f", key="meta_input", disabled=not acesso_permitido)
                
                submitted = st.form_submit_button("✅ Adicionar Operação", use_container_width=True, disabled=not acesso_permitido)
                
                if submitted and acesso_permitido:
                    nova_operacao = pd.DataFrame({
                        'OPERAÇÃO': [operacao],
                        'OPERAÇÃO TITULAR': [operacao_titular],
                        'MARCA': [marca],
                        'MODELO': [modelo],
                        'TIPO': [tipo],
                        'META': [meta],
                        'DATA_CRIACAO': [datetime.now().strftime("%d/%m/%Y %H:%M:%S")],
                        'CRIADO_POR': [nome_usuario]
                    })
                    
                    df_operacoes = pd.concat([df_operacoes, nova_operacao], ignore_index=True)
                    if salvar_dados_eficiente(client, SHEET_ID, "operacoes", df_operacoes):
                        st.success("✅ Operação adicionada com sucesso!")
                        time.sleep(1)
                        st.rerun()
                elif submitted and not acesso_permitido:
                    st.error("❌ Acesso não autorizado. Digite a senha correta.")
        
        with col2:
            st.subheader("📋 Operações Cadastradas")
            
            # Botão de exportação
            if not df_operacoes.empty:
                # Converter para Excel
                output = BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_operacoes.to_excel(writer, index=False, sheet_name='Operações')
                excel_data = output.getvalue()
                
                st.download_button(
                    label="📤 Exportar para Excel",
                    data=excel_data,
                    file_name=f"operacoes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            
            if not df_operacoes.empty:
                # Formatar META com 2 casas decimais
                df_display = df_operacoes.copy()
                if 'META' in df_display.columns:
                    df_display['META'] = df_display['META'].round(2)
                
                # Exibir tabela
                st.dataframe(
                    df_display[['OPERAÇÃO', 'OPERAÇÃO TITULAR', 'MARCA', 'MODELO', 'TIPO', 'META', 'DATA_CRIACAO']],
                    use_container_width=True,
                    height=400
                )
                
                # Controles de exclusão
                st.subheader("🗑️ Excluir Operação")
                operacao_excluir = st.selectbox(
                    "Selecione a operação para excluir:",
                    options=df_operacoes['OPERAÇÃO'].tolist(),
                    key="operacao_excluir_select"
                )
                
                senha_exclusao = st.text_input("🔒 Digite a senha de administração para excluir:", type="password", key="senha_exclusao_operacao")
                
                if st.button("🗑️ Confirmar Exclusão", use_container_width=True, disabled=not senha_exclusao):
                    if senha_exclusao == SENHA_ADMIN:
                        df_operacoes = df_operacoes[df_operacoes['OPERAÇÃO'] != operacao_excluir].reset_index(drop=True)
                        if salvar_dados_eficiente(client, SHEET_ID, "operacoes", df_operacoes):
                            st.success(f"✅ Operação {operacao_excluir} excluída com sucesso!")
                            time.sleep(1)
                            st.rerun()
                    else:
                        st.error("❌ Senha incorreta. Não é possível excluir.")
            else:
                st.info("Nenhuma operação cadastrada ainda.")

    # ----------------------- REGISTROS -----------------------
    elif "📝 Registros" in menu:
        st.markdown('<h1 class="main-header">Registro de Atendimentos</h1>', unsafe_allow_html=True)
        
        # Estado da sessão para controle das seleções
        if 'operacao_selecionada' not in st.session_state:
            st.session_state.operacao_selecionada = None
        if 'placa_digitada' not in st.session_state:
            st.session_state.placa_digitada = ""
        if 'ocultar_info' not in st.session_state:
            st.session_state.ocultar_info = False
        
        # Layout em 3 colunas
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            st.subheader("🚗 Informações do Veículo")
            
            # Campo de placa com validação
            placa_digitada = st.text_input("🔢 DIGITE A PLACA", value=st.session_state.placa_digitada, 
                                         placeholder="Ex: ABC1234", key="placa_input")
            
            # Validação da placa
            veiculo_info = None
            if placa_digitada:
                st.session_state.placa_digitada = placa_digitada
                veiculo_encontrado = df_veiculos[df_veiculos['PLACA'].str.upper() == placa_digitada.upper()]
                
                if not veiculo_encontrado.empty:
                    veiculo_info = veiculo_encontrado.iloc[0]
                    st.success(f"✅ Placa encontrada: {placa_digitada.upper()}")
                    
                    # Exibir informações do veículo
                    marca_value = veiculo_info.get("MARCA", "")
                    modelo_value = veiculo_info.get("MODELO", "")
                    tipo_value = veiculo_info.get("TIPO", "")
                    operacao_value = veiculo_info.get("OPERAÇÃO", "")
                    
                    st.text_input("🏭 MARCA", value=marca_value, disabled=True, key="marca_veiculo")
                    st.text_input("🔧 MODELO", value=modelo_value, disabled=True, key="modelo_veiculo")
                    st.text_input("📋 TIPO", value=tipo_value, disabled=True, key="tipo_veiculo")
                    st.text_input("🏢 OPERAÇÃO", value=operacao_value, disabled=True, key="operacao_veiculo")
                    
                else:
                    st.error("❌ Placa não encontrada. Verifique o cadastro do veículo.")
            
            # Campo de motorista livre
            motorista = st.text_input("👤 MOTORISTA", placeholder="Digite o nome do motorista", key="motorista_input")
        
        with col2:
            st.subheader("📋 Informações da Abordagem")
            data_abordagem = st.date_input("📅 DATA DE ABORDAGEM", value=datetime.today(), key="data_abordagem")
            revisao = st.selectbox("🔧 REVISÃO", options=["REVISÃO EM DIA", "PENDENTE"], key="revisao_select")
            tacografo = st.selectbox("📊 TACÓGRAFO", options=["TACÓGRAFO EM DIA", "PENDENTE"], key="tacografo_select")
            
            # Observação
            observacao = st.text_area("📝 OBSERVAÇÃO", placeholder="Digite observações relevantes sobre o atendimento...", 
                                    height=100, key="observacao_text")
        
        with col3:
            st.subheader("⏰ Período do Atendimento")
            data_inicio = st.date_input("📅 DATA INÍCIO", value=datetime.today(), key="data_inicio")
            data_fim = st.date_input("📅 DATA FIM", value=datetime.today() + timedelta(days=7), key="data_fim")
            
            # Média de atendimento
            media_atendimento = st.number_input("⭐ MÉDIA ATENDIMENTO", min_value=0.0, format="%.2f", key="media_atendimento")
        
        # SELEÇÃO DE OPERAÇÃO (abaixo das 3 colunas)
        st.subheader("🏢 Seleção de Operação")
        
        # Barra de pesquisa para operação titular
        pesquisa_operacao = st.text_input("🔍 Pesquisar por Operação Titular:", 
                                        placeholder="Digite o nome da operação titular",
                                        key="pesquisa_operacao")
        
        # Filtrar operações com base na pesquisa
        df_operacoes_filtrado = df_operacoes.copy()
        if pesquisa_operacao:
            df_operacoes_filtrado = df_operacoes_filtrado[
                df_operacoes_filtrado['OPERAÇÃO TITULAR'].str.contains(pesquisa_operacao, case=False, na=False) |
                df_operacoes_filtrado['OPERAÇÃO'].str.contains(pesquisa_operacao, case=False, na=False)
            ]
        
        # Tabela de operações para seleção
        st.markdown("**📋 Selecione uma operação:**")
        
        # Preparar dados para exibição
        operacoes_display = df_operacoes_filtrado[['OPERAÇÃO', 'OPERAÇÃO TITULAR', 'MARCA', 'MODELO', 'TIPO', 'META']].copy()
        operacoes_display['META'] = operacoes_display['META'].round(2)
        operacoes_display['SELECIONAR'] = False
        
        # Adicionar índice para seleção
        operacoes_display['ID'] = range(1, len(operacoes_display) + 1)
        
        # Criar interface de seleção
        edited_df = st.data_editor(
            operacoes_display[['SELECIONAR', 'ID', 'OPERAÇÃO', 'OPERAÇÃO TITULAR', 'MARCA', 'MODELO', 'TIPO', 'META']],
            hide_index=True,
            use_container_width=True,
            height=200,
            column_config={
                "SELECIONAR": st.column_config.CheckboxColumn(
                    "Selecionar",
                    help="Selecione a operação",
                    default=False,
                    width="small"
                ),
                "ID": st.column_config.NumberColumn(
                    "ID",
                    help="Identificador",
                    width="small"
                ),
                "OPERAÇÃO": st.column_config.TextColumn(
                    "Operação",
                    width="medium"
                ),
                "OPERAÇÃO TITULAR": st.column_config.TextColumn(
                    "Titular",
                    width="medium"
                ),
                "MARCA": st.column_config.TextColumn(
                    "Marca",
                    width="small"
                ),
                "MODELO": st.column_config.TextColumn(
                    "Modelo",
                    width="small"
                ),
                "TIPO": st.column_config.TextColumn(
                    "Tipo",
                    width="small"
                ),
                "META": st.column_config.NumberColumn(
                    "Meta",
                    format="%.2f",
                    width="small"
                )
            },
            disabled=["ID", "OPERAÇÃO", "OPERAÇÃO TITULAR", "MARCA", "MODELO", "TIPO", "META"],
            key="operacoes_table"
        )
        
        # Verificar qual operação foi selecionada
        operacao_selecionada = None
        operacao_info_selecionada = None
        
        for idx, row in edited_df.iterrows():
            if row['SELECIONAR']:
                operacao_selecionada = row['OPERAÇÃO']
                # Encontrar informações completas da operação selecionada
                operacao_info_selecionada = df_operacoes[df_operacoes['OPERAÇÃO'] == operacao_selecionada].iloc[0]
                break
        
        if operacao_selecionada:
            st.session_state.operacao_selecionada = operacao_selecionada
            st.success(f"✅ Operação selecionada: {operacao_selecionada}")
            
            # Exibir informações da operação selecionada
            col_info1, col_info2, col_info3 = st.columns(3)
            with col_info1:
                st.text_input("👑 OPERAÇÃO TITULAR", value=operacao_info_selecionada.get("OPERAÇÃO TITULAR", ""), disabled=True)
            with col_info2:
                st.text_input("🎯 META", value=f"{operacao_info_selecionada.get('META', 0):.2f}", disabled=True)
            with col_info3:
                st.text_input("📋 TIPO", value=operacao_info_selecionada.get("TIPO", ""), disabled=True)
        else:
            st.warning("⚠️ Selecione uma operação na tabela acima")
        
        # Botão de envio
        st.subheader("✅ Confirmar Atendimento")
        enviar = st.button("🚀 ENVIAR ATENDIMENTO", type="primary", use_container_width=True)
        
        if enviar:
            # Validações antes do envio
            if not placa_digitada or veiculo_info is None:
                st.error("❌ Por favor, digite uma placa válida cadastrada no sistema.")
            elif not st.session_state.operacao_selecionada:
                st.error("❌ Por favor, selecione uma operação.")
            elif not motorista:
                st.error("❌ Por favor, digite o nome do motorista.")
            else:
                # Buscar informações da operação selecionada
                operacao_info = df_operacoes[df_operacoes['OPERAÇÃO'] == st.session_state.operacao_selecionada].iloc[0]
                
                novo_atendimento = pd.DataFrame({
                    "MOTORISTA": [motorista],
                    "COLABORADOR": [nome_usuario],
                    "DATA_ABORDAGEM": [data_abordagem.strftime("%d/%m/%Y")],
                    "DATA_LANCAMENTO": [datetime.now().strftime("%d/%m/%Y %H:%M:%S")],
                    "PLACA": [placa_digitada.upper()],
                    "MODELO": [veiculo_info.get("MODELO", "")],
                    "REVISAO": [revisao],
                    "TACOGRAFO": [tacografo],
                    "OPERACAO": [st.session_state.operacao_selecionada],
                    "DATA_INICIO": [data_inicio.strftime("%d/%m/%Y")],
                    "DATA_FIM": [data_fim.strftime("%d/%m/%Y")],
                    "META": [operacao_info.get("META", 0)],
                    "MEDIA_ATENDIMENTO": [round(media_atendimento, 2)],
                    "OBSERVACAO": [observacao],
                    "DATA_MODIFICACAO": [datetime.now().strftime("%d/%m/%Y %H:%M:%S")],
                    "MODIFICADO_POR": [nome_usuario]
                })
                
                df_atendimentos = pd.concat([df_atendimentos, novo_atendimento], ignore_index=True)
                if salvar_dados_eficiente(client, SHEET_ID, "atendimentos", df_atendimentos):
                    st.success("✅ Atendimento registrado com sucesso!")
                    
                    # Limpar campos após envio
                    st.session_state.placa_digitada = ""
                    st.session_state.operacao_selecionada = None
                    
                    time.sleep(2)
                    st.rerun()

    # ----------------------- VEÍCULOS -----------------------
    elif "🚗 Veículos" in menu:
        st.markdown('<h1 class="main-header">Consulta de Veículos</h1>', unsafe_allow_html=True)
        
        # Indicadores de veículos
        if not df_veiculos.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown(f"""
                <div class="card-indicador">
                    <h3>🚗 Total</h3>
                    <h2>{len(df_veiculos)}</h2>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                urbanos = len(df_veiculos[df_veiculos['TIPO'] == 'URBANO']) if 'TIPO' in df_veiculos.columns else 0
                st.markdown(f"""
                <div class="card-indicador">
                    <h3>🏙️ Urbanos</h3>
                    <h2>{urbanos}</h2>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                longos = len(df_veiculos[df_veiculos['TIPO'] == 'LONGO CURSO']) if 'TIPO' in df_veiculos.columns else 0
                st.markdown(f"""
                <div class="card-indicador">
                    <h3>🛣️ Longo Curso</h3>
                    <h2>{longos}</h2>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                outros = len(df_veiculos[~df_veiculos['TIPO'].isin(['URBANO', 'LONGO CURSO'])]) if 'TIPO' in df_veiculos.columns else 0
                st.markdown(f"""
                <div class="card-indicador">
                    <h3>📦 Outros</h3>
                    <h2>{outros}</h2>
                </div>
                """, unsafe_allow_html=True)
        
        # Campo de pesquisa de placa
        st.subheader("🔍 Pesquisar Veículo")
        pesquisa_placa = st.text_input("Digite a placa para pesquisar:", placeholder="Ex: ABC1234", key="pesquisa_placa")
        
        # Botão de exportação
        if not df_veiculos.empty:
            # Converter para Excel
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_veiculos.to_excel(writer, index=False, sheet_name='Veículos')
            excel_data = output.getvalue()
            
            st.download_button(
                label="📤 Exportar para Excel",
                data=excel_data,
                file_name=f"veiculos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        
        st.subheader("📋 Veículos Cadastrados")
        if not df_veiculos.empty:
            # Aplicar filtro de pesquisa se houver
            df_display = df_veiculos.copy()
            if pesquisa_placa:
                df_display = df_display[df_display['PLACA'].str.contains(pesquisa_placa.upper(), na=False)]
            
            st.dataframe(
                df_display[['PLACA', 'MARCA', 'MODELO', 'OPERAÇÃO', 'PROPRIETÁRIO', 'TIPO', 'DATA_CADASTRO']],
                use_container_width=True,
                height=400
            )
        else:
            st.info("Nenhum veículo cadastrado ainda.")

    # ----------------------- HISTÓRICO -----------------------
    elif "📋 Histórico" in menu:
        st.markdown('<h1 class="main-header">Histórico de Atendimentos</h1>', unsafe_allow_html=True)
        
        # Indicadores de histórico
        if not df_atendimentos.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown(f"""
                <div class="card-indicador">
                    <h3>📋 Total</h3>
                    <h2>{len(df_atendimentos)}</h2>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                hoje = datetime.now().date()
                hoje_count = len(df_atendimentos[pd.to_datetime(df_atendimentos['DATA_ABORDAGEM']).dt.date == hoje])
                st.markdown(f"""
                <div class="card-indicador">
                    <h3>📅 Hoje</h3>
                    <h2>{hoje_count}</h2>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                media_geral = df_atendimentos['MEDIA_ATENDIMENTO'].mean()
                media_formatada = f"{media_geral:.2f}" if not pd.isna(media_geral) else "0.00"
                st.markdown(f"""
                <div class="card-indicador">
                    <h3>⭐ Média</h3>
                    <h2>{media_formatada}</h2>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                em_dia = len(df_atendimentos[df_atendimentos['REVISAO'] == 'REVISÃO EM DIA']) if 'REVISAO' in df_atendimentos.columns else 0
                st.markdown(f"""
                <div class="card-indicador">
                    <h3>✅ Em dia</h3>
                    <h2>{em_dia}</h2>
                </div>
                """, unsafe_allow_html=True)
        
        # Botão de exportação
        if not df_atendimentos.empty:
            # Converter para Excel
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_atendimentos.to_excel(writer, index=False, sheet_name='Atendimentos')
            excel_data = output.getvalue()
            
            st.download_button(
                label="📤 Exportar para Excel",
                data=excel_data,
                file_name=f"historico_atendimentos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        
        # Filtros avançados
        st.subheader("🔍 Filtros")
        filtros = criar_filtros_avancados(df_atendimentos, df_operacoes)
        
        # Aplicar filtros
        df_filtrado = df_atendimentos.copy()
        
        if filtros['data_range'] and len(filtros['data_range']) == 2:
            data_inicio, data_fim = filtros['data_range']
            df_filtrado = df_filtrado[
                (pd.to_datetime(df_filtrado['DATA_ABORDAGEM']).dt.date >= data_inicio) &
                (pd.to_datetime(df_filtrado['DATA_ABORDAGEM']).dt.date <= data_fim)
            ]
        
        if filtros['operacao_filtro']:
            df_filtrado = df_filtrado[df_filtrado['OPERAÇÃO TITULAR'].isin(filtros['operacao_filtro'])]
        
        if filtros['status_filtro'] and 'REVISAO' in df_filtrado.columns:
            df_filtrado = df_filtrado[df_filtrado['REVISAO'].isin(filtros['status_filtro'])]
        
        # Exibir histórico filtrado
        st.subheader("📊 Histórico de Atendimentos")
        if not df_filtrado.empty:
            # Formatar colunas numéricas
            df_display = df_filtrado.copy()
            if 'MEDIA_ATENDIMENTO' in df_display.columns:
                df_display['MEDIA_ATENDIMENTO'] = df_display['MEDIA_ATENDIMENTO'].round(2)
            if 'META' in df_display.columns:
                df_display['META'] = df_display['META'].round(2)
            
            # Aplicar formatação condicional para META vs MEDIA_ATENDIMENTO
            def color_meta(row):
                if pd.notna(row['MEDIA_ATENDIMENTO']) and pd.notna(row['META']):
                    if row['MEDIA_ATENDIMENTO'] >= row['META']:
                        return ['background-color: #d4edda; color: #155724; font-weight: bold'] * len(row)
                    else:
                        return ['background-color: #f8d7da; color: #721c24; font-weight: bold'] * len(row)
                return [''] * len(row)
            
            # Exibir dados com formatação
            st.dataframe(
                df_display[[
                    'PLACA', 'MOTORISTA', 'DATA_ABORDAGEM', 'OPERACAO', 
                    'OPERAÇÃO TITULAR', 'MEDIA_ATENDIMENTO', 'META', 'REVISAO', 'COLABORADOR'
                ]].style.apply(color_meta, axis=1),
                use_container_width=True,
                height=400
            )
            
            # Controles de exclusão
            st.subheader("🗑️ Excluir Atendimento")
            atendimentos_options = [f"{i+1} - {row['PLACA']} - {row['DATA_ABORDAGEM']}" for i, row in df_filtrado.iterrows()]
            
            if atendimentos_options:
                atendimento_excluir = st.selectbox(
                    "Selecione o atendimento para excluir:",
                    options=atendimentos_options,
                    key="atendimento_excluir_select"
                )
                
                if atendimento_excluir:
                    # Extrair o índice do atendimento selecionado
                    selected_index = atendimentos_options.index(atendimento_excluir)
                    original_idx = df_filtrado.index[selected_index]
                    
                    senha_exclusao = st.text_input("🔒 Digite a senha de administração para excluir:", type="password", key="senha_exclusao_atendimento")
                    
                    if st.button("🗑️ Confirmar Exclusão", use_container_width=True, disabled=not senha_exclusao):
                        if senha_exclusao == SENHA_ADMIN:
                            df_atendimentos = df_atendimentos.drop(original_idx).reset_index(drop=True)
                            if salvar_dados_eficiente(client, SHEET_ID, "atendimentos", df_atendimentos):
                                st.success(f"✅ Atendimento excluído com sucesso!")
                                time.sleep(1)
                                st.rerun()
                        else:
                            st.error("❌ Senha incorreta. Não é possível excluir.")
        else:
            st.info("Nenhum atendimento encontrado com os filtros aplicados.")

if __name__ == "__main__":
    main()
