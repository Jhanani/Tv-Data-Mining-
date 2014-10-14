import sys
import csv

def create_plot_table(csv_file):
	plot_file = 'plot_'+csv_file
	with open(csv_file,'rb') as rfile:
		with open(plot_file,'wb') as wfile:
			reader = csv.reader(rfile)
			writer = csv.writer(wfile)
			line = reader.next()
			colnames = line[23:33]
			colvalues = list()
			rownames = list()
			for i in range(0,5):
				line = reader.next()
				rownames.append(line[1])
				colvalues.append(line[23:33])
			writer.writerow(['']+rownames)
			print len(rownames)
			print len(colnames)
			print len(colvalues)
			print len(colvalues[0])
			for i in range(0,len(colnames)):
				writer.writerow([colnames[i],colvalues[0][i],colvalues[1][i],colvalues[2][i],colvalues[3][i],colvalues[4][i]])

if __name__ == '__main__':
	create_plot_table(sys.argv[1])
