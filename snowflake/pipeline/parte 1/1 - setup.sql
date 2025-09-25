-- =====================================================================
-- SCRIPT DE SETUP PARA O PROJETO DE POLÍTICAS PÚBLICAS DE ENERGIA
-- IE - INTELIGENCIA ENERGETICA
-- Finalidade: Infraestrutura para a Pipeline de Dados
-- Responsável: Ronen Filho
-- Data: 24/09/2025
-- =====================================================================

-- 1. Setup de Objetos (Warehouse, Database, Schemas)
-- ---------------------------------------------------------------------

-- Warehouse dedicado para cargas de trabalho de transformação e ELT
CREATE WAREHOUSE IF NOT EXISTS IE_TRANSFORM_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 300 -- Suspensão em 5 minutos para economizar custos
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse para as cargas de transformação de dados do projeto Inteligencia Energetica.';

-- Database central para o projeto
CREATE DATABASE IF NOT EXISTS IE_DB
  COMMENT = 'Database com dados brutos (staging) e tratados (core) para o projeto Inteligencia Energetica.';

USE DATABASE IE_DB;

-- Schemas para organizar as etapas da pipeline
CREATE SCHEMA IF NOT EXISTS STAGING
  COMMENT = 'Schema para dados brutos ou semi-estruturados, antes da transformação.';

CREATE SCHEMA IF NOT EXISTS CORE
  COMMENT = 'Schema para modelos de dados de negócio, prontos para consumo (tabelas largas).';


-- 2. Setup de Acessos (Role, Usuário de Serviço)
-- ---------------------------------------------------------------------

-- Role (papel) que conterá todas as permissões para ferramentas de transformação
CREATE ROLE IF NOT EXISTS IE_TRANSFORM_ROLE;

CREATE USER IF NOT EXISTS SVC_PIPELINE_USER_IE
  LOGIN_NAME = 'svc_pipeline_user_ie'
  DEFAULT_ROLE = IE_TRANSFORM_ROLE
  DEFAULT_WAREHOUSE = 'IE_TRANSFORM_WH'
  DEFAULT_NAMESPACE = 'IE_DB.STAGING'
  PASSWORD = 'tFmrErTepy5KWGndBFdb'
  COMMENT = 'Usuário de serviço para ferramentas da pipeline de dados do projeto Inteligencia Energetica.';

GRANT ROLE IE_TRANSFORM_ROLE TO USER SVC_PIPELINE_USER_IE;
GRANT ROLE IE_TRANSFORM_ROLE TO USER RONEN;


-- 3. Concessão de Permissões para o Papel de Transformação
-- ---------------------------------------------------------------------
USE ROLE ACCOUNTADMIN; -- Use um papel com privilégios para conceder permissões

-- Permissões de uso dos objetos principais
GRANT USAGE ON WAREHOUSE IE_TRANSFORM_WH TO ROLE IE_TRANSFORM_ROLE;
GRANT OPERATE ON WAREHOUSE IE_TRANSFORM_WH TO ROLE IE_TRANSFORM_ROLE;
GRANT USAGE ON DATABASE IE_DB TO ROLE IE_TRANSFORM_ROLE;

-- ***** AJUSTE CHAVE - PERMISSÃO PARA AIRBYTE FUNCIONAR *****
GRANT CREATE SCHEMA ON DATABASE IE_DB TO ROLE IE_TRANSFORM_ROLE;
-- **********************************************************

-- Permissões nos schemas já existentes (para dbt e outras ferramentas)
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE FUNCTION ON SCHEMA IE_DB.STAGING TO ROLE IE_TRANSFORM_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE FUNCTION ON SCHEMA IE_DB.CORE TO ROLE IE_TRANSFORM_ROLE;

-- Permissões para manipular os dados nas tabelas (atuais e futuras)
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA IE_DB.STAGING TO ROLE IE_TRANSFORM_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA IE_DB.CORE TO ROLE IE_TRANSFORM_ROLE;