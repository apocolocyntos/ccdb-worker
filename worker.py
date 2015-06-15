#!/usr/bin/python

import sys,os,time,uuid,tarfile,ConfigParser
import json,couchdb,datetime,socket,random,shutil



def get_queued_calculations(db):
	map_fun = ''' function(doc) { if(doc.calculation.state=='queued') { emit(doc._id); } } '''
	results = db.query(map_fun)
	calculations = []
	for item in results:
		calculations.append(db.get(item.key))
	return calculations



def upload_output(db,doc,log_file):
	f = open(log_file,'r')
	db.put_attachment(doc,f,"log")
	return

def make_tarfile(output_filename, source_dir):
		with tarfile.open(output_filename, "w:gz") as tar:
				tar.add(source_dir, arcname=os.path.basename(source_dir))





# ORCA specific

def run_orca(program,job_dir,calculation_setup):
	# get setup from document
	scf_type = calculation_setup['scf_type']
	functional = calculation_setup['functional']
	basis_set = calculation_setup['basis_set']
	charge = str(calculation_setup['charge'])
	multiplicity = str(calculation_setup['multiplicity'])
	# write input file
	f = open(job_dir + '/' + input_file, 'w+')
	f.write("! " + scf_type + " " + functional + " " + basis_set)
	if calculation_setup['optimization']:
		f.write(" opt")
	if calculation_setup['frequency']:
		f.write(" freq")
	f.write("\n")
	f.write("* xyz " + charge + " " + multiplicity + "\n")
	for item in calculation_setup['coordinates']:
		 f.write(item['element'] + " " + str(item['x']) + " " + str(item['y']) + " " + str(item['z']) + "\n")
	f.write("*\n")
	f.close()
	# run program
	os.system("nohup " + program + " " + job_dir + "/" + input_file + " > " + job_dir + "/" + output_file)
	f = open(job_dir + '/' + output_file, 'r')
	if '                             ****ORCA TERMINATED NORMALLY****\n' in f.readlines():
		return True
	else:
		return False




# Main


# read settings
config_file = open(os.path.split(os.path.abspath(sys.argv[0]))[0] + '/config.json','r')
settings = json.loads(config_file.read())



# database setup
host = settings['database']['host']
port = settings['database']['port']
db_name = settings['database']['database']
user = settings['database']['user']
password = settings['database']['password']



# connect to server and database
url = 'http://' + user + ':' + password + '@' + host + ':' + port + '/'
server = couchdb.Server(url)
db = server[db_name]



work_dir = settings['jobs']['directory']
orca = settings['programs']['orca']['path']
input_file = "job.inp"
output_file = "job.out"




utc_datetime = datetime.datetime.utcnow()


while True:
	
	calculation_finished = False
	queued_calculations = get_queued_calculations(db)
	
	# check for new jobs
	
	if len(queued_calculations) == 0:
		print "no calculation queued"
		time.sleep(10)

	else:
		index = random.randrange(0,len(queued_calculations))
		doc = queued_calculations[index]
		doc['calculation']['state'] = "in_progress"
		doc['calculation']['start_time'] = utc_datetime.strftime("%Y-%m-%d-%H-%M-%S")
		db.save(doc)
		
		job_dir = work_dir + '/' + doc['_id']
		
		# run the program

		if not os.path.exists(job_dir):
			os.makedirs(job_dir)
		
		if doc['calculation']['program']['name'] == "orca":
			calculation_finished = run_orca(orca,job_dir,doc['setup'])
		
		
		# write results to the database
		
		doc = db.get(doc['_id'])
		
		if calculation_finished:
			doc['calculation']['state'] = 'finished'
		else:
			doc['calculation']['state'] = 'error'
			doc['calculation']['fail_count'] += 1
		
		doc['calculation']['end_time'] = time.strftime("%Y-%m-%d-%H-%M-%S")
		doc['calculation']['user'] = user
		db.save(doc)

		# upload document to database
		upload_output(db,doc,job_dir + '/' + output_file)

		# archive the work folder
		if calculation_finished:
			f = open(job_dir + '/' + 'id','w')
			f.write(doc['_id'])
			f.close()
			make_tarfile(job_dir + '.tar.gz', job_dir)
		shutil.rmtree(job_dir)