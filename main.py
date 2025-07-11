from dataclasses import dataclass
import io
import struct
import sys
TAM_CAB_BUCKETS:int = 2 # Tamanho do cabeçalho do arquivo de buckets
TAM_MAX_BUCKET:int = 5
FORMAT_CAMPO:str = "I"
FORMAT_CAB:str = "H"
TAM_CAMPO:int = 4
ARQUIVO_BUCKETS:str = "buckets.dat"
TAM_REG_BUCKET = TAM_CAMPO*2 + TAM_MAX_BUCKET*TAM_CAMPO

@dataclass
class Diretorio:
    profundidade:int = 0
    buckets = []

class Bucket:
    def __init__(self,ref=None,profundidade=None,quant_chaves=None,chaves=None):
        if ref:
            self.ref = ref
        else:
            self.ref = 0

        if profundidade:
            self.profundidade = profundidade
        else:
            self.profundidade = 0
        
        if quant_chaves:
            self.quant_chaves = quant_chaves
        else:
            self.quant_chaves = 0
        
        if chaves:
            self.chaves = chaves
        else:
            self.chaves = [-1]*TAM_MAX_BUCKET
        
    ref:int = 0
    profundidade:int = 0 # 4 bytes
    quant_chaves:int = 0 # 4 bytes
    chaves = [0]*TAM_MAX_BUCKET # 4 bytes * TAM_MAX_BUCKET

# Inicialização ====================================================================================================================
def inicializar_diretorio():
    dir:Diretorio = Diretorio() # Cria o diretório
    
    arq:io.TextIOWrapper = open(ARQUIVO_BUCKETS,"wb+")
    arq.write(int.to_bytes(1,TAM_CAB_BUCKETS,signed=True))
        
    novo_bucket:Bucket = Bucket()
    novo_bucket.profundidade = dir.profundidade
    novo_bucket.ref = 0
    escrever_bucket(novo_bucket)

    dir.buckets.append(0)
    arq.close()
    return dir

# Funções buckets ===============================================================================================================
def criar_bucket(dir:Diretorio):
    arq:io.TextIOWrapper = open(ARQUIVO_BUCKETS,"rb+")
    cab = arq.read(TAM_CAB_BUCKETS)
    quant_buckets = 0
    if cab:
        quant_buckets = int.from_bytes(cab)    
        arq.seek((TAM_REG_BUCKET*quant_buckets)+TAM_CAB_BUCKETS)
    else:
        arq.write(int.to_bytes(0,TAM_CAB_BUCKETS))

    novo_bucket:Bucket = Bucket()
    novo_bucket.profundidade = dir.profundidade
    novo_bucket.ref = quant_buckets

    arq.write(int.to_bytes(novo_bucket.profundidade,TAM_CAMPO,signed=True))
    arq.write(int.to_bytes(novo_bucket.quant_chaves,TAM_CAMPO))

    for _ in range(TAM_MAX_BUCKET):
        arq.write(int.to_bytes(0,TAM_CAMPO,signed=True))

    arq.seek(0)
    arq.write(int.to_bytes(quant_buckets+1,TAM_CAB_BUCKETS))
    arq.close()
    return novo_bucket

def carrega_bucket(rrn:int):
    arq:io.TextIOWrapper = open(ARQUIVO_BUCKETS,"rb+")
    bucket_carregado:Bucket = Bucket()
    
    posicao = (rrn*TAM_REG_BUCKET)+TAM_CAB_BUCKETS
    arq.seek(posicao)

    bucket_carregado.profundidade = int.from_bytes(arq.read(TAM_CAMPO),signed=True)
    
    if(bucket_carregado.profundidade == -1): # Bucket excluído
        return None
    
    bucket_carregado.quant_chaves = int.from_bytes(arq.read(TAM_CAMPO),signed=False)
    bucket_carregado.ref = rrn

    for i in range(bucket_carregado.quant_chaves):
        bucket_carregado.chaves[i] = int.from_bytes(arq.read(TAM_CAMPO),signed=True)
    return bucket_carregado

def dividir_bucket(bucket:Bucket,diretorio:Diretorio):
    if bucket.profundidade == diretorio.profundidade:
        expandir_diretorio(diretorio)
    novo_bucket:Bucket = criar_bucket(diretorio)
    inicio, fim = encontrar_novo_intervalo(bucket,diretorio.profundidade)

    for i in range(inicio,fim+1):
        diretorio.buckets[i] = novo_bucket.ref

    bucket.profundidade += 1
    novo_bucket.profundidade = bucket.profundidade
    escrever_bucket(novo_bucket)
    redistribuir_chaves(bucket,diretorio)
    
def buscar_chave_bucket(chave:int, bucket:Bucket):
    arq:io.TextIOBase = open(ARQUIVO_BUCKETS,"rb")
    arq.seek(posicao_bucket(bucket)+TAM_CAMPO*2)

    encontrado = int.from_bytes(arq.read(TAM_CAMPO),signed=True)
    pos_chave = 0

    while encontrado != None and pos_chave < bucket.quant_chaves:
        if encontrado == chave:
            return True, pos_chave # Chave, Foi encontrada?, posição onde encontrou
        else:
            pos_chave += 1
            encontrado = int.from_bytes(arq.read(TAM_CAMPO),signed=True)
    return False, 0

def excluir_chave_bucket(chave:int, bucket:Bucket):
    chave_encontrada, pos = buscar_chave_bucket(chave,bucket)
    if chave_encontrada:
        bucket.chaves[pos] = 0
        bucket.quant_chaves -= 1
        deslocar_chaves(bucket.chaves,pos)
        escrever_bucket(bucket)
        return True
    else:
        return False

def posicao_bucket(bucket:Bucket):
    return TAM_REG_BUCKET*bucket.ref+TAM_CAB_BUCKETS

def deslocar_chaves(chaves:list[int],posicao:int):
    for i in range(posicao,len(chaves)-1):
        chaves[i] = chaves[i+1]

def escrever_bucket(bucket:Bucket):
    buckets:io.TextIOWrapper = open(ARQUIVO_BUCKETS,"rb+")
    pos = posicao_bucket(bucket)
    buckets.seek(pos)
    buckets.write(int.to_bytes(bucket.profundidade,TAM_CAMPO))
    buckets.write(int.to_bytes(bucket.quant_chaves,TAM_CAMPO))

    for i in range(TAM_MAX_BUCKET):
        buckets.write(int.to_bytes(bucket.chaves[i],TAM_CAMPO,signed=True))
    buckets.close()

# Funções diretório ==========================================================================================================
def expandir_diretorio(dir:Diretorio):
    novos_buckets = []

    for i in range(len(dir.buckets)):
        novos_buckets += [dir.buckets[i]]*2
    dir.buckets = novos_buckets
    dir.profundidade+=1

def buscar_chave_diretorio(chave:int,diretorio:Diretorio):
    endereco = gerar_endereco(chave,diretorio.profundidade)
    bucket = carrega_bucket(diretorio.buckets[endereco])
    
    if bucket == None: return False, None
    
    encontrada,_ = buscar_chave_bucket(chave,bucket)
    return encontrada, bucket

def printar_diretorio(dir:Diretorio):
    print("---- Diretório ----")
    i = 0
    for bucket in dir.buckets:
        
        bk = carrega_bucket(bucket)
        print(f"dir[{i}] -> bucket{bk.ref}:{bk.chaves}")
        i+=1

# Funções de inserção =============================================================================================================
def gerar_endereco(chave:int,profundidade:int):
    val_ret = 0
    mascara = 1
    hash_val = hash(chave)

    for _ in range(profundidade):
        val_ret = val_ret << 1
        bit_baixa_ordem = hash_val & mascara # Ultimo bit do hash
        val_ret = val_ret | bit_baixa_ordem # Adiciona o ultimo bit
        hash_val = hash_val >> 1 # Passa para o próximo bit
    return val_ret

def inserir_chave(chave:int,dir:Diretorio):
    chave_existe,_ = buscar_chave_diretorio(chave,dir)
    if chave_existe: 
        print("Chave duplicada")
        return False # Chave duplicada

    end = gerar_endereco(chave,dir.profundidade)
    bucket:Bucket = carrega_bucket(dir.buckets[end])

    if bucket.quant_chaves < TAM_MAX_BUCKET: # Insere
        bucket.chaves[bucket.quant_chaves] = chave
        bucket.quant_chaves += 1
        escrever_bucket(bucket)
    else:
        dividir_bucket(bucket,dir)
        inserir_chave(chave,dir)
    printar_diretorio(dir)
    return True

def redistribuir_chaves(bucket:Bucket,diretorio:Diretorio):
    chaves = bucket.chaves[0:bucket.quant_chaves]
    for chave in chaves:
        excluir_chave_bucket(chave,bucket) # Esvazia o bucket
    for chave in chaves:
        inserir_chave(chave,diretorio) # Redistribui

def encontrar_novo_intervalo(bucket:Bucket,dir_prof:int):
    mascara = 1
    chave = bucket.chaves[0]
    end_comum = gerar_endereco(chave, bucket.profundidade)
    end_comum = end_comum << 1
    end_comum = end_comum | mascara
    bits_a_preencher = dir_prof - (bucket.profundidade + 1)
    novo_inicio, novo_fim = end_comum, end_comum
    for _ in range(bits_a_preencher):
        novo_inicio = novo_inicio << 1
        novo_fim = novo_fim << 1
        novo_fim = novo_fim | mascara

    return novo_inicio, novo_fim

# Funções de remoção ===========================================================================================================

def excluir_chave(chave:int,dir:Diretorio):
    endereco = gerar_endereco(chave,dir.profundidade)
    bucket:Bucket = carrega_bucket(dir.buckets[endereco])

    if bucket == None: return False

    if excluir_chave_bucket(chave,bucket):
        tentar_combinar_buckets(bucket,endereco,dir)
        return True
    return False

def tentar_combinar_buckets(bucket:Bucket,endereco:int,dir:Diretorio):
    tem_amigo, amigo = encontrar_bucket_amigo(bucket,dir)
    if tem_amigo:
        bucket_amigo = carrega_bucket(dir.buckets[amigo])
        if(bucket_amigo.quant_chaves + bucket.quant_chaves <= TAM_MAX_BUCKET):
            combinado = concatena_buckets(bucket,bucket_amigo)
            excluir_bucket(bucket_amigo)
            escrever_bucket(combinado)
            dir.buckets[amigo] = dir.buckets[endereco]
            

            if tentar_reduzir_diretorio(dir):
                for i in range(0,len(dir.buckets),2):
                    tentar_combinar_buckets(carrega_bucket(dir.buckets[i]),i,dir)
            return True
    return False

def excluir_bucket(bucket:Bucket):
    arq:io.TextIOWrapper = open(ARQUIVO_BUCKETS,"rb+")
    arq.seek(posicao_bucket(bucket))
    arq.write(int.to_bytes(-1,TAM_CAMPO,signed=True))

def concatena_buckets(bucket1:Bucket,bucket2:Bucket):
    novo_bucket = Bucket()
    novo_bucket.profundidade = bucket1.profundidade - 1
    novo_bucket.quant_chaves = bucket1.quant_chaves + bucket2.quant_chaves
    novo_bucket.ref = bucket1.ref
    novo_bucket.chaves = bucket1.chaves[0:bucket1.quant_chaves] + bucket2.chaves[0:bucket2.quant_chaves]
    novo_bucket.chaves += [0]*(TAM_MAX_BUCKET-novo_bucket.quant_chaves)
    
    return novo_bucket

def encontrar_bucket_amigo(bucket:Bucket,dir:Diretorio):
    if dir.profundidade == 0: return None, False
    if bucket.profundidade < dir.profundidade: return None, False

    end_comum = gerar_endereco(bucket.chaves[0],bucket.profundidade)
    end_amigo = end_comum ^ 1
    return True, end_amigo 

def tentar_reduzir_diretorio(dir:Diretorio):
    novas_referencias = []
    if dir.profundidade == 0: return False

    for i in range(0,len(dir.buckets),2):
        if dir.buckets[i] == dir.buckets[i+1]:
            novas_referencias.append(dir.buckets[i])
        else:
            return False
    # Reduzindo
    dir.buckets = novas_referencias
    dir.profundidade -= 1

    return not tentar_reduzir_diretorio(dir)

# Funções de execução =========================================================================================================

def executar_operacoes(nome_arquivo:str,dir:Diretorio):
    try:
        arq:io.TextIOWrapper = open(nome_arquivo,"r")
        log:io.TextIOWrapper = open(f"log-{nome_arquivo}","w")
    except FileNotFoundError:
        print(f"Erro: Arquivo \"{nome_arquivo}\"não encontrado.")
        return
    
    linha = arq.readline()
    
    while linha:
        campos = linha.split(" ")
        operacao = campos[0]
        entrada = int(campos[1])
        match operacao:
            case "i":
                log.write(f"Inserção da chave {entrada}: ")
                if inserir_chave(entrada,dir):
                    log.write("Sucesso.\n")
                else:
                    log.write("Falha – Chave duplicada.\n")
            case "b":
                encontrou, bucket = buscar_chave_diretorio(entrada,dir)
                log.write(f"Busca pela chave {entrada}: ")
                if encontrou:
                    log.write(f"Chave encontrada no bucket {bucket.ref}.\n")
                else:
                    log.write("Chave não encontrada.\n")
            case "r":
                log.write(f"Remoção da chave {entrada}: ")
                if excluir_chave(entrada,dir):
                    log.write(f"Sucesso.\n")
                else:
                    log.write(f"Falha - Chave não encontrada.\n")
        linha = arq.readline()
    arq.close()
    log.close()
            

def main():
    diretorio = inicializar_diretorio()
    args:list[str] = sys.argv
    if len(args) <= 2: raise Exception("Argumentos inválidos.\n Uso do programa: [-e, -pd, -pb]") 
    op = args[1]

    match op:
        case "-e":
            if len(args) != 3: raise Exception("Argumentos inválidos.\n Uso: -e [arquivo-operacoes]")
            executar_operacoes(args[2],diretorio)
        case "-pd":
            pass
        case "-pb":
            pass
    printar_diretorio(diretorio)

if __name__ == "__main__":
    main()