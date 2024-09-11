# Project Name: df_fraud_challenge

## Descrição
Esse projeto faz uma ingestão orquestrada por Airflow cujo objetivo é gerar as seguintes tabelas resultado:
- Listar em ordem decrescente as “location_region” por média de “risk score”
- Considerando somente a transação mais recente ("timestamp") com
"transaction_type" igual a "sale" de cada "receiving address", liste os 3
"receiving address" com maior "amount" dentre estas transações (apresente
o "receiving address", o "amount" e o "timestamp").

## Sumário
- [Instalação](#Instalação)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Instalação
To get started with this project, follow these steps:
1. Clone the repository: `git clone https://github.com/your-username/df_fraud_challenge.git`
2. Install the required dependencies: `pip install -r requirements.txt`

## Usage
To use this project, follow these steps:
1. Navigate to the project directory: `cd df_fraud_challenge`
2. Run the main script: `python main.py`
3. Follow the prompts and provide the necessary inputs.

## Contributing
Contributions are welcome! If you would like to contribute to this project, please follow these guidelines:
1. Fork the repository.
2. Create a new branch: `git checkout -b feature/your-feature-name`
3. Make your changes and commit them: `git commit -m 'Add your feature description'`
4. Push to the branch: `git push origin feature/your-feature-name`
5. Submit a pull request.

## License
This project is licensed under the [MIT License](LICENSE).
