var cassandraDriver = require('cassandra-driver');
var client = new cassandraDriver.Client({
  contactPoints: ['localhost:9042'],
  localDataCenter: 'datacenter1'
});
const prompt = require('prompt-sync')();

async function  printPoliceman() {
var query;
  query = "SELECT * FROM Policja.policjant";
   client.execute(query, function(e, res) {
    console.log("Policjanci:", res );
  });
}

async function addPoliceman () {
  console.log('Dodaj policjanta')
  const id = prompt('ID: ')
  const imie = prompt('Imie: ')
  const nazwisko = prompt('Nazwisko: ')
  const stopien = prompt('Stopień: ')
  const wydzial = prompt('Wydział: ')
  const wiek = prompt('Wiek: ')
    const query = [
      {
        query: 'INSERT INTO Policja.policjant(id, imie, nazwisko, stopien, wydzial, wiek) VALUES (?, ?, ?, ?, ?, ?)',
        params: [ id, imie, nazwisko, stopien, wydzial, wiek ]
      }
    ];
  try {
    await client.batch(query, { prepare: true });
    console.log('Dodano policjanta');
  } catch (error) {
  console.error(error)
  }
}

async function deletePoliceman () {
  console.log('Usuń policjanta')
  const id = prompt('ID: ')
    const query = [
      {
        query: 'DELETE FROM Policja.policjant WHERE ID = ?',
        params: [ id]
      }
    ];
  try {
    await client.batch(query, { prepare: true });
    console.log('Usunięto policjanta');
  } catch (error) {
  console.error(error)
  }
}

async function updatePoliceman () {
  console.log('Aktualizuj dane policjanta')
  const id = prompt('ID: ')
  const imie = prompt('Imie: ')
  const nazwisko = prompt('Nazwisko: ')
  const stopien = prompt('Stopień: ')
  const wydzial = prompt('Wydział: ')
  const wiek = prompt('Wiek: ')
    const query = [
      {
        query: 'UPDATE Policja.policjant SET imie = ?, nazwisko =?, stopien =?, wydzial =?, wiek=?  WHERE ID = ?',
        params: [ imie, nazwisko, stopien, wydzial, wiek, id]
      }
    ];
  try {
    await client.batch(query, { prepare: true });
    console.log('Update pomyślny!');
  } catch (error) {
  console.error(error)
  }
}

const printPolicemanById = async() => {
  console.log('Wyszukaj policjanta po ID')
  const id = prompt('ID: ')
  const query = 'SELECT * FROM Policja.policjant WHERE ID = ?';
  try {
    const result = await client.execute(query, [ id ], { prepare: true });
    const row = result.first();
    console.log('Wynik:', row);
  } catch (error) {
  console.error(error)
  }
}

const printPolicemanByDepartment = async() => {
    console.log('Wyświetl policjantów z danego wydziału')
    const wydzial = prompt('Wydział: ')
    const query = 'SELECT * FROM Policja.policjant WHERE wydzial = ? ALLOW FILTERING';   
    try {
      const result = await client.execute(query, [ wydzial ], { prepare: true });
      console.log('Wynik:', result);
    } catch (error) {
    console.error(error)
    }
}

const printPolicemanByRank = async() => {
  console.log('Wyszukaj po stopniu policjanta')
  const stopien = prompt('Stopień: ')
  const query = 'SELECT * FROM Policja.policjant WHERE stopien = ? ALLOW FILTERING';
  try {
    const result = await client.execute(query, [ stopien ], { prepare: true });
    console.log('Wynik:', result);
  } catch (error) {
  console.error(error)
  }
}

client.connect(function(e) {
    var query;
    query = "CREATE KEYSPACE IF NOT EXISTS Policja WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' }";
     client.execute(query, function(e, res) {
  
    });
  });
  
createMenu = () => {
    console.log('Witaj w menu:');
    console.log('1. Wyswietl wszystkie rekordy');
    console.log('2. Dodaj ');
    console.log('3. Update ');
    console.log('4. Usun ');
    console.log('5. Wypisz po ID');
    console.log('6. Wyszukaj po wydziale');
    console.log('7. Wyszukaj po stopniu');
    console.log('8. Wyjscie');
  };

  action = (number) => {
    switch (parseInt(number)) {
      case 1:
        printPoliceman()
        break;
      case 2:
        addPoliceman()
        break;
      case 3:
        updatePoliceman()
        break;
      case 4:
        deletePoliceman()
        break;
      case 5:
        printPolicemanById()
        break;
      case 6:
        printPolicemanByDepartment()
        break; 
      case 7:
        printPolicemanByRank()
        break;
      case 8:

        break;   
    }
  }

  createMenu();
  const number = prompt('Wybrane: ');
  action(number)
  