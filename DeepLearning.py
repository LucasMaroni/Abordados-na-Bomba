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
        return obj.strftime('%Y-%m-%d %H:%M:%S')
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
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
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
    <div style="background: linear-gradient(135deg, #FF8C00 0%, #FFD700 100%); 
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
                    value=(min_date, max_date),
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
                default=operacoes_titulares[:3] if len(operacoes_titulares) > 3 else operacoes_titulares,
                key="filtro_operacao_titular"
            )
    
    with col3:
        # Filtro de status de revisão
        if not df_atendimentos.empty and 'REVISAO' in df_atendimentos.columns:
            status_options = sorted(df_atendimentos['REVISAO'].unique())
            status_filtro = st.multiselect(
                "🔧 Status Revisão",
                options=status_options,
                default=status_options,
                key="filtro_status"
            )
    
    return {
        'data_range': data_range if 'data_range' in locals() else None,
        'operacao_filtro': operacao_filtro,
        'status_filtro': status_filtro
    }

# -------------------- INTERFACE PRINCIPAL --------------------
def main():
    st.set_page_config(
        page_title="Sistema de Abordagens - Bomba",
        layout="wide", 
        page_icon="🚛",
        initial_sidebar_state="expanded"
    )
    
    # CSS Avançado para melhor UX - Tema amarelo/laranja
    st.markdown("""
    <style>
        .main-header { 
            font-size: 2.5rem; 
            color: white; 
            text-align: left; 
            margin-bottom: 2rem;
            background: linear-gradient(135deg, #FF8C00 0%, #FFD700 100%);
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
            margin-bottom: 2rem;
        }
        .logo-img {
            height: 80px;
            margin-right: -2rem;
            margin-top: -2rem;
        }
        .stButton>button {
            background: linear-gradient(135deg, #FF8C00 0%, #FFD700 100%);
            color: white;
            border: none;
            border-radius: 8px;
            padding: 0.5rem 1rem;
            font-weight: bold;
            transition: all 0.3s ease;
        }
        .stButton>button:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(255, 140, 0, 0.3);
        }
        .metric-card {
            background: linear-gradient(135deg, #FF8C00 0%, #FFD700 100%);
            padding: 1.5rem;
            border-radius: 12px;
            color: white;
            text-align: center;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        }
        .sidebar .sidebar-content {
            background: linear-gradient(180deg, #FF8C00 0%, #FFD700 100%);
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
            background: linear-gradient(135deg, #FFA500 0%, #FFD700 100%);
            color: white;
            border-radius: 8px 8px 0px 0px;
            padding: 10px 16px;
        }
        .stTabs [aria-selected="true"] {
            background: linear-gradient(135deg, #FF8C00 0%, #FFA500 100%) !important;
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
        st.markdown("""
        <div style="text-align: center; padding: 1rem;">
            <h1 style="color: white; margin-bottom: 2rem;">🚛 Sistema de Abordagens</h1>
        </div>
        """, unsafe_allow_html=True)
        
        menu = st.radio("Navegação", [
            "📊 Dashboard", "🏢 Operações", "📝 Registros", 
            "📋 Histórico", "🚗 Veículos"
        ], key="menu_navigation")
        
        st.sidebar.markdown("---")
        
        # Informações do usuário
        usuario = st.text_input("👤 Seu nome", value="Fiscal", key="user_name")
        
        if st.button("🔄 Atualizar Dados", use_container_width=True, key="refresh_button"):
            st.cache_data.clear()
            st.rerun()
        
        st.info("💡 Dados atualizados a cada 3 minutos")
    
    # ----------------------- DASHBOARD -----------------------
    if "📊 Dashboard" in menu:
        # Header com título e logo
        col_title, col_logo = st.columns([3, 1])
        with col_title:
            st.markdown('<h1 class="main-header">Dashboard de Abordados</h1>', unsafe_allow_html=True)
        with col_logo:
            # Espaço para logo - você pode substituir pela URL da sua imagem
            st.markdown("""
            <div style="text-align: right;">
                <img src="https://cdn-icons-png.flaticon.com/512/1006/1006555.png" class="logo-img" alt="Logo">
            </div>
            """, unsafe_allow_html=True)
        
        # Adicionar OPERAÇÃO TITULAR aos dados de atendimento
        if not df_atendimentos.empty and not df_operacoes.empty:
            operacao_titular_map = df_operacoes.set_index('OPERAÇÃO')['OPERAÇÃO TITULAR'].to_dict()
            df_atendimentos['OPERAÇÃO TITULAR'] = df_atendimentos['OPERACAO'].map(operacao_titular_map)
        
        # Métricas em tempo real
        col1, col2, col3, col4 = st.columns(4)
        
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
                    color_discrete_sequence=px.colors.sequential.Oranges_r
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
                    color_continuous_scale="oranges"
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
                color_continuous_scale="oranges"
            )
            fig.update_layout(yaxis_tickformat=".2f", xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        
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
                        'DATA_CRIACAO': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
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
            if not df_operacoes.empty:
                # Formatar META com 2 casas decimais
                df_display = df_operacoes.copy()
                if 'META' in df_display.columns:
                    df_display['META'] = df_display['META'].round(2)
                
                st.dataframe(
                    df_display[['OPERAÇÃO', 'OPERAÇÃO TITULAR', 'MARCA', 'MODELO', 'TIPO', 'META', 'DATA_CRIACAO']],
                    use_container_width=True,
                    height=400
                )
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
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Campo de placa com validação
            st.subheader("🚗 Informações do Veículo")
            
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
                    st.text_input("🏭 MARCA", value=veiculo_info.get("MARCA", ""), disabled=True, key="marca_veiculo")
                    st.text_input("🔧 MODELO", value=veiculo_info.get("MODELO", ""), disabled=True, key="modelo_veiculo")
                    st.text_input("📋 TIPO", value=veiculo_info.get("TIPO", ""), disabled=True, key="tipo_veiculo")
                    st.text_input("🏢 OPERAÇÃO", value=veiculo_info.get("OPERAÇÃO", ""), disabled=True, key="operacao_veiculo")
                    
                else:
                    st.error("❌ Placa não encontrada. Verifique o cadastro do veículo.")
            
            # Campo de motorista livre
            motorista = st.text_input("👤 MOTORISTA", placeholder="Digite o nome do motorista", key="motorista_input")
            
            # Informações da abordagem
            st.subheader("📋 Informações da Abordagem")
            data_abordagem = st.date_input("📅 DATA DE ABORDAGEM", value=datetime.today(), key="data_abordagem")
            revisao = st.selectbox("🔧 REVISÃO", options=["Em dia", "Atrasada", "Não se aplica"], key="revisao_select")
            tacografo = st.selectbox("📊 TACÓGRAFO", options=["OK", "Com problemas", "Não verificado"], key="tacografo_select")
        
        with col2:
            st.subheader("🏢 Seleção de Operação e Meta")
            
            # Seleção de operação com tabela
            if not df_operacoes.empty:
                # Exibir tabela de operações para seleção
                st.info("📋 Selecione uma operação na tabela abaixo:")
                
                # Preparar dados para exibição
                operacoes_display = df_operacoes[['OPERAÇÃO TITULAR', 'OPERAÇÃO', 'META', 'TIPO']].copy()
                operacoes_display['META'] = operacoes_display['META'].round(2)
                operacoes_display['SELECIONAR'] = False
                
                # Criar interface de seleção
                edited_df = st.data_editor(
                    operacoes_display,
                    hide_index=True,
                    use_container_width=True,
                    height=200,
                    column_config={
                        "SELECIONAR": st.column_config.CheckboxColumn(
                            "Selecionar",
                            help="Selecione a operação",
                            default=False,
                        ),
                        "META": st.column_config.NumberColumn(
                            format="%.2f"
                        )
                    },
                    disabled=["OPERAÇÃO TITULAR", "OPERAÇÃO", 'META', "TIPO"],
                    key="operacoes_table"
                )
                
                # Verificar qual operação foi selecionada
                operacao_selecionada = None
                meta_selecionada = 0.0
                tipo_selecionado = ""
                operacao_titular_selecionada = ""
                
                for idx, row in edited_df.iterrows():
                    if row['SELECIONAR']:
                        operacao_selecionada = row['OPERAÇÃO']
                        meta_selecionada = row['META']
                        tipo_selecionado = row['TIPO']
                        operacao_titular_selecionada = row['OPERAÇÃO TITULAR']
                        break
                
                if operacao_selecionada:
                    st.session_state.operacao_selecionada = operacao_selecionada
                    st.success(f"✅ Operação selecionada: {operacao_selecionada}")
                    
                    # Exibir informações da operação selecionada
                    st.text_input("👑 OPERAÇÃO TITULAR", value=operacao_titular_selecionada, disabled=True)
                    st.text_input("🎯 META", value=f"{meta_selecionada:.2f}", disabled=True)
                    st.text_input("📋 TIPO", value=tipo_selecionado, disabled=True)
                else:
                    st.warning("⚠️ Selecione uma operação na tabela acima")
            
            # Período do atendimento
            st.subheader("⏰ Período do Atendimento")
            data_inicio = st.date_input("📅 DATA INÍCIO", value=datetime.today(), key="data_inicio")
            data_fim = st.date_input("📅 DATA FIM", value=datetime.today() + timedelta(days=7), key="data_fim")
            
            # Média de atendimento
            media_atendimento = st.number_input("⭐ MÉDIA ATENDIMENTO", min_value=0.0, format="%.2f", key="media_atendimento")
            
            # Informações do colaborador
            st.subheader("👨‍💼 Informações do Colaborador")
            colaborador = st.text_input("🧑‍💼 COLABORADOR", value=usuario, key="colaborador_input")
            observacao = st.text_area("📝 OBSERVAÇÃO", placeholder="Digite observações relevantes sobre o atendimento...", key="observacao_text")
        
        # Botão de envio
        enviar = st.button("✅ ENVIAR ATENDIMENTO", type="primary", use_container_width=True)
        
        if enviar:
            # Validações antes do envio
            if not placa_digitada or veiculo_info is None:
                st.error("❌ Por favor, digite uma placa válida cadastrada no sistema.")
            elif not st.session_state.operacao_selecionada:
                st.error("❌ Por favor, selecione uma operação na tabela.")
            elif not motorista:
                st.error("❌ Por favor, digite o nome do motorista.")
            else:
                novo_atendimento = pd.DataFrame({
                    "MOTORISTA": [motorista],
                    "COLABORADOR": [colaborador],
                    "DATA_ABORDAGEM": [data_abordagem.strftime("%Y-%m-%d")],
                    "DATA_LANCAMENTO": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                    "PLACA": [placa_digitada.upper()],
                    "MODELO": [veiculo_info.get("MODELO", "")],
                    "REVISAO": [revisao],
                    "TACOGRAFO": [tacografo],
                    "OPERACAO": [st.session_state.operacao_selecionada],
                    "DATA_INICIO": [data_inicio.strftime("%Y-%m-%d")],
                    "DATA_FIM": [data_fim.strftime("%Y-%m-%d")],
                    "META": [meta_selecionada],
                    "MEDIA_ATENDIMENTO": [round(media_atendimento, 2)],
                    "OBSERVACAO": [observacao],
                    "DATA_MODIFICACAO": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                    "MODIFICADO_POR": [usuario]
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
        st.markdown('<h1 class="main-header">Gestão de Veículos</h1>', unsafe_allow_html=True)
        
        # Verificação de senha para modificações
        senha = st.text_input("🔒 Senha de Administração", type="password", key="senha_veiculos")
        acesso_permitido = senha == SENHA_ADMIN
        
        if not acesso_permitido and senha:
            st.error("❌ Senha incorreta. Acesso não autorizado.")
        
        # Campo de pesquisa de placa
        st.subheader("🔍 Pesquisar Veículo")
        pesquisa_placa = st.text_input("Digite a placa para pesquisar:", placeholder="Ex: ABC1234", key="pesquisa_placa")
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.subheader("➕ Adicionar Novo Veículo")
            
            with st.form("novo_veiculo", clear_on_submit=True):
                placa = st.text_input("🚗 PLACA", key="veiculo_placa", disabled=not acesso_permitido)
                marca = st.text_input("🏭 MARCA", key="veiculo_marca", disabled=not acesso_permitido)
                modelo = st.text_input("🔧 MODELO", key="veiculo_modelo", disabled=not acesso_permitido)
                operacao = st.text_input("🏢 OPERAÇÃO", key="veiculo_operacao", disabled=not acesso_permitido)
                proprietario = st.text_input("👑 PROPRIETÁRIO", key="veiculo_proprietario", disabled=not acesso_permitido)
                locado = st.selectbox("🏢 LOCADO", options=["Sim", "Não"], key="veiculo_locado", disabled=not acesso_permitido)
                motorista = st.text_input("👤 MOTORISTA", key="veiculo_motorista", disabled=not acesso_permitido)
                tipo = st.selectbox("📋 TIPO", options=["LONGO CURSO", "URBANO", "REGIONAL", "OUTRO"], key="veiculo_tipo", disabled=not acesso_permitido)
                
                submitted = st.form_submit_button("✅ Adicionar Veículo", use_container_width=True, disabled=not acesso_permitido)
                
                if submitted and acesso_permitido:
                    novo_veiculo = pd.DataFrame({
                        'PLACA': [placa.upper()],
                        'MARCA': [marca],
                        'MODELO': [modelo],
                        'OPERAÇÃO': [operacao],
                        'PROPRIETÁRIO': [proprietario],
                        'LOCADO': [locado],
                        'MOTORISTA': [motorista],
                        'TIPO': [tipo],
                        'DATA_CADASTRO': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
                    })
                    
                    df_veiculos = pd.concat([df_veiculos, novo_veiculo], ignore_index=True)
                    if salvar_dados_eficiente(client, SHEET_ID, "veiculos", df_veiculos):
                        st.success("✅ Veículo adicionado com sucesso!")
                        time.sleep(1)
                        st.rerun()
                elif submitted and not acesso_permitido:
                    st.error("❌ Acesso não autorizado. Digite a senha correta.")
        
        with col2:
            st.subheader("📋 Veículos Cadastrados")
            if not df_veiculos.empty:
                # Aplicar filtro de pesquisa se houver
                df_display = df_veiculos.copy()
                if pesquisa_placa:
                    df_display = df_display[df_display['PLACA'].str.contains(pesquisa_placa.upper(), na=False)]
                
                st.dataframe(
                    df_display[['PLACA', 'MARCA', 'MODELO', 'OPERAÇÃO', 'PROPRIETÁRIO', 'LOCADO', 'MOTORISTA', 'TIPO', 'DATA_CADASTRO']],
                    use_container_width=True,
                    height=400
                )
                
                if pesquisa_placa and len(df_display) == 0:
                    st.info("Nenhum veículo encontrado com a placa informada.")
            else:
                st.info("Nenhum veículo cadastrado ainda.")

    # ----------------------- HISTÓRICO -----------------------
    elif "📋 Histórico" in menu:
        st.markdown('<h1 class="main-header">Histórico de Atendimentos</h1>', unsafe_allow_html=True)
        
        # Adicionar OPERAÇÃO TITULAR aos dados de atendimento
        if not df_atendimentos.empty and not df_operacoes.empty:
            operacao_titular_map = df_operacoes.set_index('OPERAÇÃO')['OPERAÇÃO TITULAR'].to_dict()
            df_atendimentos['OPERAÇÃO TITULAR'] = df_atendimentos['OPERACAO'].map(operacao_titular_map)
        
        # Filtros avançados
        filtros = criar_filtros_avancados(df_atendimentos, df_operacoes)
        
        # Aplicar filtros
        if not df_atendimentos.empty:
            df_filtrado = df_atendimentos.copy()
            
            # Filtro de data
            if filtros['data_range'] and len(filtros['data_range']) == 2:
                start_date, end_date = filtros['data_range']
                df_filtrado = df_filtrado[
                    (df_filtrado['DATA_ABORDAGEM'].dt.date >= start_date) & 
                    (df_filtrado['DATA_ABORDAGEM'].dt.date <= end_date)
                ]
            
            # Filtro de operação titular
            if filtros['operacao_filtro']:
                df_filtrado = df_filtrado[df_filtrado['OPERAÇÃO TITULAR'].isin(filtros['operacao_filtro'])]
            
            # Filtro de status
            if filtros['status_filtro']:
                df_filtrado = df_filtrado[df_filtrado['REVISAO'].isin(filtros['status_filtro'])]
            
            # Estatísticas
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("📋 Total Filtrado", len(df_filtrado))
            with col2:
                st.metric("🚗 Veículos Únicos", df_filtrado['PLACA'].nunique())
            with col3:
                if not df_filtrado.empty and 'MEDIA_ATENDIMENTO' in df_filtrado.columns:
                    media_filtrada = df_filtrado['MEDIA_ATENDIMENTO'].mean()
                    media_formatada = f"{media_filtrada:.2f}" if not pd.isna(media_filtrada) else "0.00"
                    st.metric("⭐ Média Filtrada", media_formatada)
                else:
                    st.metric("⭐ Média Filtrada", "0.00")
            
            # Formatar médias para exibição
            df_display = df_filtrado.copy()
            if 'MEDIA_ATENDIMENTO' in df_display.columns:
                df_display['MEDIA_ATENDIMENTO'] = df_display['MEDIA_ATENDIMENTO'].round(2)
            if 'META' in df_display.columns:
                df_display['META'] = df_display['META'].round(2)
            
            # Tabela de dados
            st.dataframe(
                df_display.sort_values('DATA_ABORDAGEM', ascending=False),
                use_container_width=True,
                height=400
            )
        else:
            st.info("Nenhum atendimento registrado ainda.")

if __name__ == "__main__":
    main()
