#coding:utf-8
"""neo4j batch data import"""
import os
import pandas as pd
from py2neo import Node, Relationship, Graph
remote_graph = Graph("http://neo4j:Ks2018@192.168.1.74:7474")
remote_graph.delete_all()

def read_file(file_name):
	"""read data file
	   file_name:item excel format file"""
	item = pd.read_excel(file_name,sheet_name="Item").fillna("NULL").values
	symptom = pd.read_excel(file_name,sheet_name="Symptom").fillna("NULL").values
	expert = pd.read_excel(file_name,sheet_name="Expert").fillna("NULL").values
	technology = pd.read_excel(file_name, sheet_name="Technology").fillna("NULL").values
	material = pd.read_excel(file_name,sheet_name="Material").fillna("NULL").values
	instrument = pd.read_excel(file_name,sheet_name="Instrument").fillna("NULL").values
	return item,symptom,expert,technology,material,instrument

def create_graph(item_,symptom_,expert_,technology_,material_,instrument_):
	"""create graph
	   item_:项目，expert_:专家，technology_:技术，material_:材料，instrument_:仪器"""
	#Item节点
	if len(item_[0]) == 13:
		item = Node("Item",name = item_[0,0].replace(' ','') if item_ [0,0] != "NULL" else None,expert_info = item_[0,1].replace(' ','') if item_ [0,1] != "NULL" else None,
	            price = item_[0,2].replace(' ','') if item_ [0,2] != "NULL" else None,discount = item_[0,3].replace(' ','') if item_ [0,3] != "NULL" else None,
	            promotio = item_[0,4].replace(' ','') if item_ [0,4] != "NULL" else None,ask_symptoms = item_[0,5].replace(' ','') if item_ [0,5] != "NULL" else None,
	            ask_experience = item_[0,6].replace(' ','') if item_ [0,6] != "NULL" else None,introduce = item_[0,7].replace(' ','') if item_ [0,7] != "NULL" else None,
	            advantage = item_[0,8].replace(' ','') if item_ [0,8] != "NULL" else None,preparation = item_[0,9].replace(' ','') if item_ [0,9] != "NULL" else None,
	            postoperation = item_[0,10].replace(' ','') if item_ [0,10] != "NULL" else None,part = item_[0,11].replace(' ','') if item_ [0,11] != "NULL" else None,
	            process = item_[0,12].replace(' ','') if item_ [0,12] != "NULL" else None)
	else:
		item = Node("Item", name=item_[0, 0].replace(' ','') if item_[0, 0] != "NULL" else None,expert_info=item_[0, 1].replace(' ','') if item_[0, 1] != "NULL" else None,
		            price=item_[0, 2].replace(' ','') if item_[0, 2] != "NULL" else None,discount=item_[0, 3].replace(' ','') if item_[0, 3] != "NULL" else None,
		            promotio=item_[0, 4].replace(' ','') if item_[0, 4] != "NULL" else None,ask_symptoms=item_[0, 5].replace(' ','') if item_[0, 5] != "NULL" else None,
		            ask_experience=item_[0, 6].replace(' ','') if item_[0, 6] != "NULL" else None,introduce=item_[0, 7].replace(' ','') if item_[0, 7] != "NULL" else None,
		            advantage=item_[0, 8].replace(' ','') if item_[0, 8] != "NULL" else None,preparation=item_[0, 9].replace(' ','') if item_[0, 9] != "NULL" else None,
		            postoperation=item_[0, 10].replace(' ','') if item_[0, 10] != "NULL" else None,process=item_[0, 11].replace(' ','') if item_[0, 11] != "NULL" else None)

	#创建Item和Symptom关系
	for m in range(symptom_.shape[0]):
		symptom = Node("Symptom",name = symptom_[m,1].replace(' ','') if symptom_[m,1] != "NULL" else None,introduce = symptom_[m,2].replace(' ','') if symptom_[m,2] != "NULL" else None)
		r5 = Relationship(item,'HASSYMPTOM',symptom)
		remote_graph.create(r5)
	#创建Item和Expert关系
	for i in range(expert_.shape[0]):
		expert = Node("Expert", name=expert_[i, 1].replace(' ','') if expert_[i, 1] != "NULL" else None , introduce=expert_[i, 2].replace(' ','') if expert_[i, 2] != "NULL" else None)
		r1 = Relationship(item, 'HASEXPERT', expert)
		remote_graph.create(r1)
	# 创建Item和Technology关系
	for j in range(technology_.shape[0]):
		technology = Node("Technology", name = technology_[j, 1].replace(' ','') if technology_[j, 1] != "NULL" else None, introduce = technology_[j, 2].replace(' ','') if technology_[j, 2] != "NULL" else None,
		                  advantage = technology_[j, 3] if technology_[j, 3] != "NULL" else None,price = technology_[j,4].replace(' ','') if technology_[j, 4] != "NULL" else None)
		r2 = Relationship(item, 'HASTECHNOLOGY', technology)
		remote_graph.create(r2)
		#创建Technology和Material关系
		for k in range(material_.shape[0]):
			if material_[k,1] == technology_[j,1]:
				material = Node("Material",name = material_[k,2].replace(' ','') if material_[k,2] !="NULL" else None,introduce = material_[k,3].replace(' ','') if material_[k,3] !="NULL" else None,
				                advantage = material_[k,4].replace(' ','') if material_[k,4] !="NULL" else None,price = material_[k,5].replace(' ','') if material_[k,5] !="NULL" else None)
				r3 = Relationship(technology,'HASMATERIAL',material)
				remote_graph.create(r3)
		# 创建Technology和Instrument关系
		for l in range(instrument_.shape[0]):
			if instrument_[l,1] == technology_[j,1]:
				instrument = Node("Instrument",name = instrument_[l,2].replace(' ','') if instrument_[l,2] != "NULL" else None,introduce = instrument_[l,3].replace(' ','') if instrument_[l,3] != "NULL" else None,
				                  price = instrument_[l,4].replace(' ','') if instrument_[l,4] != "NULL" else None,advantage = instrument_[l,5].replace(' ','') if instrument_[l,5] != "NULL" else None)
				r4 = Relationship(technology,'HASINSTRUMENT',instrument)
				remote_graph.create(r4)
	return item
if __name__ == "__main__":
	path = "D:\知识图谱\知识抽取结果"     #项目路径
	hospital = "武汉美莱"         #医院名称
	items = []
	enterprise = Node('Enterprise', name=hospital)
	#遍历医院下的项目
	for item_file in os.listdir(path):
		file = os.path.join(path,item_file)
		item,symptom,expert, technology, material, instrument = read_file(file)
		Itm = create_graph(item,symptom,expert,technology,material,instrument)
		items.append(Itm)
	#创建Enterprise和Item关系
	for tm in items:
		r = Relationship(enterprise, 'HASITEM', tm)
		remote_graph.create(r)







