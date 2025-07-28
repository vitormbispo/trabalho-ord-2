from dataclasses import dataclass
import io
import struct
import sys

FORMAT_CAMPO:str = "i"
FORMAT_CAB:str = "Hi"
TAM_CAB_BUCKETS:int = 8 # Tamanho do cabeçalho do arquivo de buckets
TAM_MAX_BUCKET:int = 5
TAM_CAMPO:int = 4
QUANT_CAMPOS_BUCKET:int = TAM_MAX_BUCKET+2
TAM_REG_BUCKET:int = QUANT_CAMPOS_BUCKET*TAM_CAMPO

ARQUIVO_BUCKETS:str = "buckets.dat"
ARQUIVO_DIRETORIO:str = "diretorio.dat"

@dataclass
class Diretorio:
    profundidade:int = 0
    quant_buckets:int = 0
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
        
    ref:int
    profundidade:int
    quant_chaves:int
    chaves:list

# Inicialização ====================================================================================================================
def inicializar_diretorio() -> Diretorio:
    dir:Diretorio = Diretorio() # Cria o diretório
    
    arq = open(ARQUIVO_BUCKETS,"wb+")
    arq.write(struct.pack(FORMAT_CAB,1,-1))
        
    novo_bucket:Bucket = Bucket()
    escrever_bucket(novo_bucket)

    dir.buckets = [0]
    dir.quant_buckets = 1
    arq.close()
    return dir

# Funções buckets ===============================================================================================================
def criar_bucket(dir:Diretorio) -> Bucket:
    arq:io.TextIOWrapper = open(ARQUIVO_BUCKETS,"rb+")
    cab = struct.unpack(FORMAT_CAB,arq.read(TAM_CAB_BUCKETS))
    quant_buckets = cab[0]
    ped = cab[1]

    novo_bucket:Bucket = Bucket()
    novo_bucket.profundidade = dir.profundidade
    
    if ped != -1:
        novo_bucket.ref = ped
        arq.seek((ped*TAM_REG_BUCKET)+TAM_CAB_BUCKETS)
        prox = struct.unpack(f"2{FORMAT_CAMPO}",arq.read(2*TAM_CAMPO))[1]
        arq.seek(0)
        arq.write(struct.pack(FORMAT_CAB,quant_buckets,prox))
    else:
        novo_bucket.ref = quant_buckets
        arq.seek(0)
        arq.write(struct.pack(FORMAT_CAB,quant_buckets+1,ped))

    arq.seek(posicao_bucket(novo_bucket))
    arq.write(struct.pack(f"{QUANT_CAMPOS_BUCKET}"+FORMAT_CAMPO,novo_bucket.profundidade,novo_bucket.quant_chaves,*novo_bucket.chaves))

    dir.quant_buckets += 1

    arq.close()
    return novo_bucket

def carregar_bucket(rrn:int) -> Bucket|None:
    arq:io.TextIOWrapper = open(ARQUIVO_BUCKETS,"rb+")
    bucket_carregado:Bucket = Bucket()
    bucket_carregado.ref = rrn

    arq.seek(posicao_bucket(bucket_carregado))

    buffer = arq.read(TAM_REG_BUCKET)
    pacote = struct.unpack(f"{QUANT_CAMPOS_BUCKET}"+FORMAT_CAMPO,buffer)

    bucket_carregado.profundidade = pacote[0]
    
    if(bucket_carregado.profundidade == -1): # Bucket excluído
        return None
    
    bucket_carregado.quant_chaves = pacote[1]
    bucket_carregado.chaves = list(pacote[2:])
    
    arq.close()
    return bucket_carregado

def dividir_bucket(bucket:Bucket,diretorio:Diretorio) -> None:
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
    
def buscar_chave_bucket(chave:int, bucket:Bucket) -> tuple[bool,int]:
    try:
        indice = bucket.chaves.index(chave,0,bucket.quant_chaves)
        return True, indice
    except ValueError:
        return False, 0

def excluir_chave_bucket(chave:int, bucket:Bucket) -> bool:
    chave_encontrada, pos = buscar_chave_bucket(chave,bucket)
    if chave_encontrada:
        bucket.chaves[pos] = -1
        bucket.quant_chaves -= 1
        deslocar_chaves(bucket.chaves,pos)
        escrever_bucket(bucket)
        return True
    else:
        return False
    
def deslocar_chaves(chaves:list[int],posicao:int) -> None:
    for i in range(posicao,len(chaves)-1):
        chaves[i] = chaves[i+1]
        chaves[i+1] = -1

def posicao_bucket(bucket:Bucket) -> int:
    return TAM_REG_BUCKET * bucket.ref + TAM_CAB_BUCKETS

def escrever_bucket(bucket:Bucket) -> None:
    arq:io.TextIOWrapper = open(ARQUIVO_BUCKETS,"rb+")
    pos = posicao_bucket(bucket)
    arq.seek(pos)
    arq.write(struct.pack(f"{QUANT_CAMPOS_BUCKET}"+FORMAT_CAMPO,bucket.profundidade,bucket.quant_chaves,*bucket.chaves))

    arq.close()

# Funções diretório ==========================================================================================================
def expandir_diretorio(dir:Diretorio) -> None:
    novos_buckets = []

    for i in range(tamanho_diretorio(dir)):
        novos_buckets += [dir.buckets[i]]*2
    dir.buckets = novos_buckets
    dir.profundidade+=1

def tamanho_diretorio(dir:Diretorio) -> int:
    return 2 ** dir.profundidade

def buscar_chave_diretorio(chave:int,diretorio:Diretorio) -> tuple[bool,Bucket|None]:
    endereco = gerar_endereco(chave,diretorio.profundidade)
    bucket = carregar_bucket(diretorio.buckets[endereco])
    
    if bucket == None: return False, None
    
    encontrada,_ = buscar_chave_bucket(chave,bucket)
    return encontrada, bucket
 
def escrever_diretorio(dir:Diretorio) -> None:
    arq:io.TextIOWrapper = open(ARQUIVO_DIRETORIO,"wb")
    arq.write(struct.pack(f"{2+tamanho_diretorio(dir)}"+FORMAT_CAMPO,dir.profundidade,dir.quant_buckets,*dir.buckets))
    arq.close()

def carregar_diretorio(nome_arquivo:str) -> Diretorio:
    arq:io.TextIOWrapper
    try:
        arq = open(nome_arquivo,"rb")
    except FileNotFoundError:
        print(f"Erro: arquivo \"diretorio.dat\" não foi encontrado")
        return None
    
    diretorio_carregado:Diretorio = Diretorio()
    diretorio_carregado.profundidade = struct.unpack(FORMAT_CAMPO,arq.read(TAM_CAMPO))[0]
    diretorio_carregado.quant_buckets = struct.unpack(FORMAT_CAMPO,arq.read(TAM_CAMPO))[0]
    
    chaves = struct.unpack(f"{tamanho_diretorio(diretorio_carregado)}"+FORMAT_CAMPO,arq.read(TAM_CAMPO*(tamanho_diretorio(diretorio_carregado))))
    diretorio_carregado.buckets = list(chaves)

    arq.close()
    return diretorio_carregado
    
# Funções de inserção =============================================================================================================
def gerar_endereco(chave:int,profundidade:int) -> int:
    val_ret = 0
    mascara = 1
    hash_val = hash(chave)

    for _ in range(profundidade):
        val_ret = val_ret << 1
        bit_baixa_ordem = hash_val & mascara # Ultimo bit do hash
        val_ret = val_ret | bit_baixa_ordem # Adiciona o ultimo bit
        hash_val = hash_val >> 1 # Passa para o próximo bit
    return val_ret

def inserir_chave(chave:int,dir:Diretorio) -> bool:
    chave_existe,_ = buscar_chave_diretorio(chave,dir)
    if chave_existe: 
        return False # Chave duplicada

    end = gerar_endereco(chave,dir.profundidade)
    bucket:Bucket = carregar_bucket(dir.buckets[end])

    if bucket.quant_chaves < TAM_MAX_BUCKET: # Insere
        bucket.chaves[bucket.quant_chaves] = chave
        bucket.quant_chaves += 1
        escrever_bucket(bucket)
    else:
        dividir_bucket(bucket,dir)
        inserir_chave(chave,dir)
    return True

def redistribuir_chaves(bucket:Bucket,diretorio:Diretorio) -> None:
    chaves = bucket.chaves[0:bucket.quant_chaves]
    for chave in chaves:
        excluir_chave_bucket(chave,bucket) # Esvazia o bucket
    for chave in chaves:
        inserir_chave(chave,diretorio) # Redistribui

def encontrar_novo_intervalo(bucket:Bucket,dir_prof:int) -> tuple[int,int]:
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
def excluir_chave(chave:int,dir:Diretorio) -> bool:
    endereco = gerar_endereco(chave,dir.profundidade)
    bucket:Bucket = carregar_bucket(dir.buckets[endereco])
    
    if bucket == None: return False

    if excluir_chave_bucket(chave,bucket):
        tentar_combinar_buckets(bucket,endereco,dir)
        return True
    return False

def tentar_combinar_buckets(bucket:Bucket,endereco:int,dir:Diretorio) -> bool:
    tem_amigo, amigo = encontrar_bucket_amigo(bucket,dir,endereco)
    if tem_amigo:
        bucket_amigo = carregar_bucket(dir.buckets[amigo])
        if(bucket_amigo.quant_chaves + bucket.quant_chaves <= TAM_MAX_BUCKET):
            combinado = concatena_buckets(bucket,bucket_amigo)
            excluir_bucket(bucket_amigo)
            dir.quant_buckets -= 1
            escrever_bucket(combinado) 
            dir.buckets[amigo] = dir.buckets[endereco]

            if tentar_reduzir_diretorio(dir):
                for i in range(0,len(dir.buckets),2):
                    tentar_combinar_buckets(carregar_bucket(dir.buckets[i]),i,dir)
            return True
    return False

def excluir_bucket(bucket:Bucket) -> None:
    arq:io.TextIOWrapper = open(ARQUIVO_BUCKETS,"rb+")
    cab = struct.unpack(FORMAT_CAB,arq.read(TAM_CAB_BUCKETS))
    quant_buckets = cab[0]
    ped = cab[1]

    arq.seek(posicao_bucket(bucket))
    arq.write(struct.pack(f"2{FORMAT_CAMPO}",-1,ped))
    arq.seek(0)
    arq.write(struct.pack(FORMAT_CAB,quant_buckets,bucket.ref))
    arq.close()

def concatena_buckets(bucket1:Bucket,bucket2:Bucket) -> Bucket:
    novo_bucket = Bucket()
    novo_bucket.profundidade = bucket1.profundidade - 1
    novo_bucket.quant_chaves = bucket1.quant_chaves + bucket2.quant_chaves
    novo_bucket.ref = bucket1.ref
    novo_bucket.chaves = bucket1.chaves[0:bucket1.quant_chaves] + bucket2.chaves[0:bucket2.quant_chaves]
    novo_bucket.chaves += [-1]*(TAM_MAX_BUCKET-novo_bucket.quant_chaves)
    
    return novo_bucket

def encontrar_bucket_amigo(bucket:Bucket,dir:Diretorio,endereco:int) -> tuple[bool,int]:
    if dir.profundidade == 0: return False, None
    if bucket.profundidade < dir.profundidade: return False, None

    end_amigo = endereco ^ 1
    return True, end_amigo 

def tentar_reduzir_diretorio(dir:Diretorio) -> bool:
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
def executar_operacoes(nome_arquivo:str,dir:Diretorio) -> None:
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
                log.write(f"> Inserção da chave {entrada}: ")
                if inserir_chave(entrada,dir):
                    log.write("Sucesso.\n")
                else:
                    log.write("Falha – Chave duplicada.\n")
            case "b":
                encontrou, bucket = buscar_chave_diretorio(entrada,dir)
                log.write(f"> Busca pela chave {entrada}: ")
                if encontrou:
                    log.write(f"Chave encontrada no bucket {bucket.ref}.\n")
                else:
                    log.write("Chave não encontrada.\n")
            case "r":
                log.write(f"> Remoção da chave {entrada}: ")
                if excluir_chave(entrada,dir):
                    log.write(f"Sucesso.\n")
                else:
                    log.write(f"Falha - Chave não encontrada.\n")
        linha = arq.readline()
    log.write(f"As operações do arquivo {nome_arquivo} foram executadas com sucesso!")
    arq.close()
    log.close()
    
    escrever_diretorio(dir)

# Funções de log ============================================================================================================
def escrever_log_diretorio(dir:Diretorio) -> None:
    log:io.TextIOWrapper = open(f"log-pd-bk{TAM_MAX_BUCKET}.txt","w")
    log.write("----- Diretório -----\n")
    
    i = 0
    for bucket in dir.buckets:
        bk = carregar_bucket(bucket)
        log.write((f"dir[{i}] = bucket[{bk.ref}]\n"))
        i+=1
    log.write(f"\nProfundidade = {dir.profundidade}\nTamanho atual = {len(dir.buckets)}\nTotal de buckets = {dir.quant_buckets}\n")
    log.close()

def escrever_log_buckets() -> bool:
    log:io.TextIOWrapper = open(f"log-pb-bk{TAM_MAX_BUCKET}.txt","w")
    
    try:
        arq:io.TextIOWrapper = open("buckets.dat","rb")
    except FileNotFoundError:
        return False
    
    tam, ped = struct.unpack(FORMAT_CAB,arq.read(TAM_CAB_BUCKETS))
    log.write("------- PED -------\n")
    log.write(f"RRN Topo: {ped}\n\n")
    log.write("----- Buckets -----\n")
    
    for i in range(tam):
        log.write(f"Bucket {i} ")
        bucket:Bucket = carregar_bucket(i)

        if bucket == None:
            log.write("--> Removido\n\n")
            continue
        
        log.write(f"(Prof = {bucket.profundidade}):\nContaChaves = {bucket.quant_chaves}\nChaves = {bucket.chaves}\n\n")
    log.close()
    return True


def main() -> None:  
    args:list[str] = sys.argv
    if len(args) < 2: raise Exception("Argumentos inválidos.\n Uso do programa: [-e, -pd, -pb]") 
    op = args[1]

    match op:
        case "-e":
            if len(args) != 3: raise Exception("Argumentos inválidos.\n Uso: -e [arquivo-operacoes]")
            diretorio = inicializar_diretorio()
            executar_operacoes(args[2],diretorio)
        case "-pd":
            diretorio = carregar_diretorio(ARQUIVO_DIRETORIO)
            if diretorio == None:
                print("O arquivo \"diretorio.dat\" não foi encontrado")
                quit()

            escrever_log_diretorio(diretorio)
        case "-pb":
            if not escrever_log_buckets():
                print("O arquivo \"buckets.dat\" não foi encontrado")
                quit()
            
if __name__ == "__main__":
    main()