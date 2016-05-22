
/*! \example dictionary_compressed_column.hpp
 * This is an example of how to implement a compression technique in our framework. One has to inherit from an abstract base class CoGaDB::CompressedColumn and implement the pure virtual methods.
 */

#pragma once

#include <core/compressed_column.hpp>

namespace CoGaDB{
	

/*!
 *  \brief     This class represents a dictionary compressed column with type T, is the base class for all compressed typed column classes.
 */	
template<class T>
class DictionaryCompressedColumn : public CompressedColumn<T>{
	public:
	/***************** constructors and destructor *****************/
	DictionaryCompressedColumn(const std::string& name, AttributeType db_type);
	virtual ~DictionaryCompressedColumn();

	virtual bool insert(const boost::any& new_Value);
	virtual bool insert(const T& new_value);
	template <typename InputIterator>
	bool insert(InputIterator first, InputIterator last);

	virtual bool update(TID tid, const boost::any& new_value);
	virtual bool update(PositionListPtr tid, const boost::any& new_value);	
	
	virtual bool remove(TID tid);
	//assumes tid list is sorted ascending
	virtual bool remove(PositionListPtr tid);
	virtual bool clearContent();

	virtual const boost::any get(TID tid);
	//virtual const boost::any* const getRawData()=0;
	virtual void print() const throw();
	virtual size_t size() const throw();
	virtual unsigned int getSizeinBytes() const throw();

	virtual const ColumnPtr copy() const;

	virtual bool store(const std::string& path);
	virtual bool load(const std::string& path);


	
	virtual T& operator[](const int index);

    /*! values*/
    std::map<char, T> dictionary;
    std::vector<char> columnEntries;
    std::string _name;

};


/***************** Start of Implementation Section ******************/

	
	template<class T>
    DictionaryCompressedColumn<T>::DictionaryCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type), dictionary(), columnEntries(), _name(name) {

    }

	template<class T>
	DictionaryCompressedColumn<T>::~DictionaryCompressedColumn(){

	}

	template<class T>
    bool DictionaryCompressedColumn<T>::insert(const boost::any&){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return false;
	}

	template<class T>
    bool DictionaryCompressedColumn<T>::insert(const T& new_value) {
        for(unsigned int i = 0; i < columnEntries.size(); i++) {
            if(dictionary.at(columnEntries[i]) == new_value) {
                columnEntries.push_back(columnEntries[i]);
                return true;
            }
        }

        columnEntries.push_back(dictionary.size());
        dictionary.insert(std::pair<char, T>(dictionary.size(), new_value));

        return true;
	}

	template <typename T> 
	template <typename InputIterator>
    bool DictionaryCompressedColumn<T>::insert(InputIterator, InputIterator){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
		return true;
	}

	template<class T>
    const boost::any DictionaryCompressedColumn<T>::get(TID){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return boost::any();
	}

	template<class T>
	void DictionaryCompressedColumn<T>::print() const throw(){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
	}

	template<class T>
	size_t DictionaryCompressedColumn<T>::size() const throw(){
        return columnEntries.size();
	}

	template<class T>
    const ColumnPtr DictionaryCompressedColumn<T>::copy() const{
        return ColumnPtr(new DictionaryCompressedColumn<T>(*this));
	}

	template<class T>
    bool DictionaryCompressedColumn<T>::update(TID tid, const boost::any& new_value){
        if(tid >= columnEntries.size()) {
            return false;
        }

        for(unsigned int i = 0; i < columnEntries.size(); i++) {
            if(dictionary.at(columnEntries[i]) == boost::any_cast<T>(new_value)) {
                columnEntries[tid] = columnEntries[i];
                return true;
            }
        }

        columnEntries[tid] = dictionary.size();
        dictionary.insert(std::pair<char, T>(dictionary.size(), boost::any_cast<T>(new_value)));
        return true;
	}

	template<class T>
    bool DictionaryCompressedColumn<T>::update(PositionListPtr, const boost::any&){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return false;
	}
	
	template<class T>
    bool DictionaryCompressedColumn<T>::remove(TID tid){
        if(tid >= columnEntries.size()) {
            return false;
        }

        columnEntries.erase(columnEntries.begin() + tid);
        return true;
	}
	
	template<class T>
	bool DictionaryCompressedColumn<T>::remove(PositionListPtr){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
		return false;			
	}

	template<class T>
	bool DictionaryCompressedColumn<T>::clearContent(){
        columnEntries.clear();
        return true;
	}

	template<class T>
    bool DictionaryCompressedColumn<T>::store(const std::string& path_){
        // Recreate normal/standard column form?

        std::string path(path_);
        path += _name;

//        std::cout << "Writing Column '" << _name << "' to File '" << path << "'" << std::endl;

        std::ofstream outfile (path.c_str(), std::fstream::out | std::fstream::binary);
        boost::archive::binary_oarchive oa(outfile);

//        oa << columnEntries;

        // REMAPS TO THE INITIAL VALUES. PLACEHOLDER UNTIL REAL REQUIREMENTS FOR THIS METHOD ARE CLEAR TO US.
        std::vector<T> simpleColumnRepresentation;
        for(unsigned int i = 0; i < columnEntries.size(); i++) {
            simpleColumnRepresentation.push_back(dictionary.at(columnEntries[i]));
        }
        oa << simpleColumnRepresentation;

        outfile.flush();
        outfile.close();

//        for(unsigned int i = 0; i < x.size(); i++) {
//            std::cout << "Column entry[" << i << "]: " << x[i] << std::endl;
//        }

        return true;
	}

	template<class T>
    bool DictionaryCompressedColumn<T>::load(const std::string& path_){
        std::string path(path_);
        path += _name;

        std::ifstream infile (path.c_str(), std::fstream::in | std::fstream::binary);
        boost::archive::binary_iarchive ia(infile);

//        ia >> columnEntries;
        infile.close();

        return true;
	}

	template<class T>
    T& DictionaryCompressedColumn<T>::operator[](const int index){
        char key = columnEntries[index];
        return dictionary.at(key);
	}

	template<class T>
	unsigned int DictionaryCompressedColumn<T>::getSizeinBytes() const throw(){
        // What shall be returned? Size of columnEntries and dictionary?
        return columnEntries.capacity() * sizeof(T);
	}

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

