import zipfile
import os

def extract_zip(zip_path, destination_dir):
    try:
        # Verifica se o arquivo ZIP existe
        if not os.path.exists(zip_path):
            print(f"Erro: O arquivo {zip_path} não foi encontrado.")
            return False

        # Cria o diretório de destino se ele não existir
        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)
            print(f"Diretório criado: {destination_dir}")

        print(f"Extraindo {zip_path} para {destination_dir}...")
        
        # Abre o arquivo ZIP em modo de leitura
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Extrai todos os conteúdos do ZIP para o diretório de destino
            zip_ref.extractall(destination_dir)
            
        print("Extração concluída!")
        return True
    
    except zipfile.BadZipFile:
        print(f"Erro: O arquivo {zip_path} não é um arquivo ZIP válido.")
        return False
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
        return False

def main():
    """
    Função principal para demonstrar a extração com caminho relativo.
    """
    # Define o nome do arquivo e o diretório de destino
    zip_file_name = "cnes_estabelecimentos_csv.zip"
    relative_download_dir = os.path.join("..", "cnes/download")
    
    # Monta o caminho completo para o arquivo ZIP
    zip_file_path = os.path.join(relative_download_dir, zip_file_name)
    
    # Define o caminho para a pasta onde os arquivos serão extraídos
    # Exemplo: cria uma subpasta 'extraido' dentro da pasta 'downloads'
    extraction_dir = os.path.join(relative_download_dir, "extraido")

    if extract_zip(zip_file_path, extraction_dir):
        print("Extração do arquivo ZIP foi um sucesso.")
    else:
        print("A extração do arquivo ZIP falhou.")

if __name__ == "__main__":
    main()