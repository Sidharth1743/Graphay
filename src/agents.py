from langchain_core.prompts import ChatPromptTemplate, PromptTemplate , MessagesPlaceholder
# replaced GoogleGenerativeAIEmbeddings due to protobuf runtime issues
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_chroma import Chroma
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from .structure import *
from .prompts import *
from dotenv import load_dotenv

load_dotenv()

class Agents:
    def __init__(self):
        llm = ChatOpenAI(model = "gpt-4o", temperature = 0)

        embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        vectorstore = Chroma(persist_directory="db", embedding_function=embeddings)
        retriever = vectorstore.as_retriever(search_kwargs={"k": 3})

        email_category_prompt = PromptTemplate(
            template=CATEGORIZE_EMAIL_PROMPT, 
            input_variables=["email"]
        )
        self.categorize_email = (
            email_category_prompt | 
            llm.with_structured_output(CategorizeEmailOutput)
        )
        generate_query_prompt = PromptTemplate(
            template=GENERATE_RAG_QUERIES_PROMPT, 
            input_variables=["email"]
        )
        self.design_rag_queries     = (
            generate_query_prompt | 
            llm.with_structured_output(RAGQueriesOutput)
        )

        qa_prompt = ChatPromptTemplate.from_template(GENERATE_RAG_ANSWER_PROMPT)
        self.generate_rag_answer = (
            {"context" : retriever , "question": RunnablePassthrough()}
            | qa_prompt 
            | llm
            | StrOutputParser
        )

        writer_prompt = ChatPromptTemplate.from_messages(
            [
                ("system", EMAIL_WRITER_PROMPT),
                MessagesPlaceholder("history"),
                ("human", "{email_information}")
            ]
        )
        self.email_writer = (
            writer_prompt | 
            llm.with_structured_output(WriterOutput)
        )   

        proofreader_prompt = PromptTemplate(
            template=EMAIL_PROOFREADER_PROMPT, 
            input_variables=["initial_email", "generated_email"]
        )
        self.email_proofreader = (
            proofreader_prompt | 
            llm.with_structured_output(ProofReaderOutput) 
        )
